import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { Activation, ActivationDocument, ActivationStatus, HistoryEntry } from './activation.schema';
import { CreateActivationDto } from './dto/activation.dto';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import type { BillingAccountCreatedEvent, ProvisioningCompletedEvent } from '@activation-poc/contracts';

@Injectable()
export class ActivationsService {
  constructor(
    @InjectModel(Activation.name) private readonly activationModel: Model<ActivationDocument>,
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  async create(dto: CreateActivationDto): Promise<{ activationId: string; status: ActivationStatus }> {
    const activationId = `act-${uuidv4().slice(0, 8)}`;
    const now = new Date().toISOString();

    const history: HistoryEntry[] = [
      {
        eventType: 'ActivationRequested',
        at: now,
        details: { planId: dto.planId, channel: 'web' },
      },
    ];

    const activation = new this.activationModel({
      _id: activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      status: 'PENDING',
      simulateFailure: dto.simulateFailure ?? 'none',
      steps: {
        billing: { status: 'PENDING' },
        provisioning: { status: 'PENDING' },
      },
      history,
    });

    await activation.save();

    await this.kafkaProducer.publishActivationRequested(
      activationId,
      dto.customerId,
      dto.planId,
      dto.simulateFailure ?? 'none',
    );

    return { activationId, status: 'PENDING' };
  }

  async findById(id: string): Promise<ActivationDetailsResponseDto> {
    const activation = await this.activationModel.findById(id).lean().exec();
    if (!activation) {
      throw new NotFoundException(`Activation with id ${id} not found`);
    }
    return activation as unknown as ActivationDetailsResponseDto;
  }

  async handleBillingAccountCreated(event: BillingAccountCreatedEvent): Promise<void> {
    await this.recordStepResult(
      'billing',
      event.correlationId,
      event.customerId,
      { eventType: event.eventType, details: { accountId: event.payload.accountId } },
    );
  }

  async handleProvisioningCompleted(event: ProvisioningCompletedEvent): Promise<void> {
    await this.recordStepResult(
      'provisioning',
      event.correlationId,
      event.customerId,
      { eventType: event.eventType, details: { orderId: event.payload.orderId, lineId: event.payload.lineId } },
    );
  }

  private async recordStepResult(
    step: 'billing' | 'provisioning',
    activationId: string,
    customerId: string,
    historyEntry: { eventType: string; details?: Record<string, unknown> },
  ): Promise<void> {
    const now = new Date().toISOString();

    // Actualización atómica: solo actualiza si el step sigue en PENDING.
    // Devuelve el documento resultante para decidir el siguiente paso con datos frescos.
    const updated = await this.activationModel
      .findOneAndUpdate(
        { _id: activationId, [`steps.${step}.status` as const]: 'PENDING' },
        {
          $set: {
            [`steps.${step}`]: { status: 'OK', at: now },
            // El primer resultado mueve la activación a IN_PROGRESS.
            // Si el segundo llega después, este $set pisa IN_PROGRESS con el mismo valor; si llega
            // antes del cierre, el documento resultante ya tiene ambos steps OK para la decisión.
            status: 'IN_PROGRESS',
            updatedAt: now,
          },
          $push: {
            history: {
              eventType: historyEntry.eventType,
              at: now,
              details: historyEntry.details ?? {},
            },
          },
        },
        { new: true },
      )
      .lean()
      .exec();

    if (!updated) {
      // La activación no existe o ese step ya fue procesado (posible replay del mensaje); nada que hacer.
      return;
    }

    const bothOk =
      updated.steps.billing.status === 'OK' && updated.steps.provisioning.status === 'OK';

    if (!bothOk) {
      return;
    }

    // Ambos steps OK: solo uno de los dos consumidores concurrentes puede ganar esta transición
    // porque el filtro exige status IN_PROGRESS y ambos steps en OK. Mongo aplica cada
    // findOneAndUpdate de forma atómica por documento, por lo que la segunda actualización
    // concurrente encuentra status ya en ACTIVE y devuelve null. Así se publica
    // ActivationCompleted exactamente una vez.
    const completed = await this.activationModel
      .findOneAndUpdate(
        {
          _id: activationId,
          status: 'IN_PROGRESS',
          'steps.billing.status': 'OK',
          'steps.provisioning.status': 'OK',
        },
        {
          $set: { status: 'ACTIVE', updatedAt: now },
          $push: {
            history: {
              eventType: 'ActivationCompleted',
              at: now,
              details: { planId: updated.planId },
            },
          },
        },
        { new: true },
      )
      .lean()
      .exec();

    if (completed) {
      await this.kafkaProducer.publishActivationCompleted(
        activationId,
        customerId,
        updated.planId,
      );
    }
  }
}

interface ActivationDetailsResponseDto {
  _id: string;
  customerId: string;
  planId: string;
  status: ActivationStatus;
  simulateFailure: string;
  steps: {
    billing: { status: string; at?: string; reason?: string };
    provisioning: { status: string; at?: string; reason?: string };
  };
  history: Array<{ eventType: string; at: string; details?: Record<string, unknown> }>;
  createdAt: Date;
  updatedAt: Date;
}