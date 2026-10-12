import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { Activation, ActivationDocument, ActivationStatus, HistoryEntry } from './activation.schema';
import { CreateActivationDto } from './dto/activation.dto';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { ActivationsGateway } from './activations.gateway';
import { consumeIdempotently, ProcessedEvent, ProcessedEventDocument } from '@activation-poc/kafka-toolkit';
import type {
  BillingAccountCreatedEvent,
  BillingAccountCancelledEvent,
  BillingFailedEvent,
  ProvisioningCompletedEvent,
  ProvisioningFailedEvent,
} from '@activation-poc/contracts';

/**
 * Servicio de negocio de activation-api y agregador de la saga de activación.
 * Crea activaciones, consume los resultados de billing/provisioning de forma
 * idempotente (transaccional) y delimita las transiciones de estado, emitiendo
 * por WebSocket y publicando ActivationCompleted/ActivationFailed una única vez.
 */
@Injectable()
export class ActivationsService {
  constructor(
    @InjectModel(Activation.name) private readonly activationModel: Model<ActivationDocument>,
    @InjectModel(ProcessedEvent.name) private readonly processedEventModel: Model<ProcessedEventDocument>,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly gateway: ActivationsGateway,
  ) {}

  /**
   * Crea una activación en Mongo (estado PENDING), emite el evento inicial por
   * WebSocket y publica ActivationRequested en Kafka.
   * @param dto Datos de la solicitud de activación.
   * @returns ID generado y estado inicial de la activación.
   */
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

    this.gateway.emitActivationEvent({
      activationId,
      eventType: 'ActivationRequested',
      at: now,
      status: 'PENDING',
    });

    await this.kafkaProducer.publishActivationRequested(
      activationId,
      dto.customerId,
      dto.planId,
      dto.simulateFailure ?? 'none',
    );

    return { activationId, status: 'PENDING' };
  }

  /**
   * Busca una activación por ID.
   * @param id ID de la activación.
   * @returns Detalle completo de la activación.
   * @throws NotFoundException Si la activación no existe.
   */
  async findById(id: string): Promise<ActivationDetailsResponseDto> {
    const activation = await this.activationModel.findById(id).lean().exec();
    if (!activation) {
      throw new NotFoundException(`Activation with id ${id} not found`);
    }
    return activation as unknown as ActivationDetailsResponseDto;
  }

  /**
   * Maneja BillingAccountCreated: marca el paso 'billing' como OK.
   * @param event Evento de cuenta de facturación creada.
   */
  async handleBillingAccountCreated(event: BillingAccountCreatedEvent): Promise<void> {
    await this.recordStepResult({
      step: 'billing',
      stepResult: 'OK',
      reason: undefined,
      activationId: event.correlationId,
      customerId: event.customerId,
      eventId: event.eventId,
      historyEntry: {
        eventType: event.eventType,
        details: { accountId: event.payload.accountId },
      },
    });
  }

  /**
   * Maneja BillingAccountCancelled (compensación): la registra en el historial
   * y la emite por WebSocket sin cambiar el estado terminal de la saga.
   * @param event Evento de cancelación de cuenta de facturación.
   */
  async handleBillingAccountCancelled(event: BillingAccountCancelledEvent): Promise<void> {
    // La compensación no cambia el estado de la saga (ya está terminal) pero SÍ
    // queda registrada en el history y se emite por WebSocket (RF-08: la timeline
    // muestra cada evento de la activación).
    const now = new Date().toISOString();
    const outcome = await consumeIdempotently(
      this.processedEventModel,
      event.eventId,
      async (session) => {
        return this.activationModel
          .findOneAndUpdate(
            { _id: event.correlationId },
            {
              $push: {
                history: {
                  eventType: event.eventType,
                  at: now,
                  details: { accountId: event.payload.accountId, reason: event.payload.reason },
                },
              },
              $set: { updatedAt: now },
            },
            { new: true, session },
          )
          .lean()
          .exec();
      },
    );

    if (outcome.result === 'duplicate' || !outcome.value) {
      return;
    }

    this.gateway.emitActivationEvent({
      activationId: event.correlationId,
      eventType: event.eventType,
      at: now,
      status: outcome.value.status,
    });
  }

  /**
   * Maneja BillingFailed: marca el paso 'billing' como FAILED.
   * @param event Evento de fallo de facturación.
   */
  async handleBillingFailed(event: BillingFailedEvent): Promise<void> {
    await this.recordStepResult({
      step: 'billing',
      stepResult: 'FAILED',
      reason: event.payload.reason,
      activationId: event.correlationId,
      customerId: event.customerId,
      eventId: event.eventId,
      historyEntry: {
        eventType: event.eventType,
        details: { reason: event.payload.reason },
      },
    });
  }

  /**
   * Maneja ProvisioningCompleted: marca el paso 'provisioning' como OK.
   * @param event Evento de aprovisionamiento completado.
   */
  async handleProvisioningCompleted(event: ProvisioningCompletedEvent): Promise<void> {
    await this.recordStepResult({
      step: 'provisioning',
      stepResult: 'OK',
      reason: undefined,
      activationId: event.correlationId,
      customerId: event.customerId,
      eventId: event.eventId,
      historyEntry: {
        eventType: event.eventType,
        details: { orderId: event.payload.orderId, lineId: event.payload.lineId },
      },
    });
  }

  /**
   * Maneja ProvisioningFailed: marca el paso 'provisioning' como FAILED.
   * @param event Evento de fallo de aprovisionamiento.
   */
  async handleProvisioningFailed(event: ProvisioningFailedEvent): Promise<void> {
    await this.recordStepResult({
      step: 'provisioning',
      stepResult: 'FAILED',
      reason: event.payload.reason,
      activationId: event.correlationId,
      customerId: event.customerId,
      eventId: event.eventId,
      historyEntry: {
        eventType: event.eventType,
        details: { orderId: event.payload.orderId, reason: event.payload.reason },
      },
    });
  }

  /**
   * Núcleo del agregador: registra el resultado de un paso dentro de una
   * transacción idempotente y, según el resultado, actualiza el estado global
   * de la saga. Los efectos externos (WebSocket/Kafka) se ejecutan solo después
   * del commit, garantizando publicación exactamente-una-vez de los eventos finales.
   * @param params Paso afectado, resultado, evento asociado y entrada de historial.
   */
  private async recordStepResult(params: {
    step: 'billing' | 'provisioning';
    stepResult: 'OK' | 'FAILED';
    reason?: string;
    activationId: string;
    customerId: string;
    eventId: string;
    historyEntry: { eventType: string; details?: Record<string, unknown> };
  }): Promise<void> {
    const { step, stepResult, reason, activationId, customerId, eventId, historyEntry } = params;
    const now = new Date().toISOString();

    // Idempotencia con transacción: el insert de eventId en processed_events y TODAS
    // las escrituras del agregador van en la misma tx. Una entrega duplicada del mismo
    // eventId falla el insert dedup (E11000) y la tx aborta sin ejecutar el efecto:
    // nada se registra, emite ni publica dos veces.
    const outcome = await consumeIdempotently(
      this.processedEventModel,
      eventId,
      async (session) => {
        // Escritura atómica 1 — registrar el resultado del step:
        // solo corre si ese step sigue PENDING pero NO filtra por estado global:
        // si la saga ya terminó (FAILED o ACTIVE) un resultado posterior queda
        // registrado en steps e history, sin cambiar estado ni publicar nada.
        const updated = await this.activationModel
          .findOneAndUpdate(
            {
              _id: activationId,
              [`steps.${step}.status` as const]: 'PENDING',
            },
            {
              $set: {
                [`steps.${step}`]: { status: stepResult, at: now, ...(reason ? { reason } : {}) },
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
            { new: true, session },
          )
          .lean()
          .exec();

        if (!updated) {
          // La activación no existe o ese step ya fue procesado; nada que hacer
          // (el marcador de processed_events queda grabado igual: es un duplicado lógico).
          return { kind: 'noop' as const };
        }

        if (stepResult === 'FAILED') {
          // Primer fallo: solo un consumidor concurrente puede ganar esta transición
          // porque el filtro exige un estado no terminal. Mongo aplica cada
          // findOneAndUpdate de forma atómica por documento, así que el segundo
          // intento concurrente encuentra FAILED y devuelve null: ActivationFailed
          // se publica exactamente una vez.
          const failed = await this.activationModel
            .findOneAndUpdate(
              { _id: activationId, status: { $in: ['PENDING', 'IN_PROGRESS'] } },
              {
                $set: { status: 'FAILED', updatedAt: now },
                $push: {
                  history: {
                    eventType: 'ActivationFailed',
                    at: now,
                    details: { planId: updated.planId, failedStep: step, reason: reason ?? '' },
                  },
                },
              },
              { new: true, session },
            )
            .lean()
            .exec();

          return { kind: 'failed' as const, won: !!failed, planId: updated.planId, status: 'FAILED' as const };
        }

        // Resultado OK: el primer resultado mueve la saga a IN_PROGRESS (si ya está
        // más avanzada el filtro lo impide; escritura inofensiva en carrera).
        await this.activationModel
          .updateOne(
            { _id: activationId, status: 'PENDING' },
            { $set: { status: 'IN_PROGRESS', updatedAt: now } },
            { session },
          )
          .exec();

        const bothOk =
          updated.steps.billing.status === 'OK' && updated.steps.provisioning.status === 'OK';

        if (!bothOk) {
          return { kind: 'recorded' as const, status: updated.status };
        }

        // Ambos steps OK: solo uno de los dos consumidores concurrentes puede ganar
        // esta transición porque el filtro exige status IN_PROGRESS y ambos steps en
        // OK. La segunda actualización concurrente encuentra status ya en ACTIVE y
        // devuelve null: ActivationCompleted se publica exactamente una vez.
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
            { new: true, session },
          )
          .lean()
          .exec();

        return { kind: 'recorded' as const, status: updated.status, completed: !!completed, planId: updated.planId };
      },
    );

    if (outcome.result === 'duplicate' || outcome.value?.kind === 'noop') {
      return;
    }

    const result = outcome.value!;

    // Efectos externos (WebSocket y Kafka) solo después del commit de la tx.
    this.gateway.emitActivationEvent({
      activationId,
      eventType: historyEntry.eventType,
      at: now,
      status: result.status,
    });

    if (result.kind === 'failed' && result.won) {
      this.gateway.emitActivationEvent({
        activationId,
        eventType: 'ActivationFailed',
        at: now,
        status: 'FAILED',
      });
      await this.kafkaProducer.publishActivationFailed(
        activationId,
        customerId,
        result.planId,
        step,
        reason ?? '',
      );
    }

    if (result.kind === 'recorded' && result.completed) {
      this.gateway.emitActivationEvent({
        activationId,
        eventType: 'ActivationCompleted',
        at: now,
        status: 'ACTIVE',
      });
      await this.kafkaProducer.publishActivationCompleted(
        activationId,
        customerId,
        result.planId!,
      );
    }
  }
}

/** DTO interno con el detalle completo de una activación tal como se persiste. */
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