import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { BillingAccount, BillingAccountDocument } from './billing-account.schema';
import { FailedActivation, FailedActivationDocument } from './failed-activation.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import type { ActivationRequestedEvent, ActivationFailedEvent } from '@activation-poc/contracts';

@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  constructor(
    @InjectModel(BillingAccount.name)
    private readonly billingAccountModel: Model<BillingAccountDocument>,
    @InjectModel(FailedActivation.name)
    private readonly failedActivationModel: Model<FailedActivationDocument>,
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  async handleActivationRequested(event: ActivationRequestedEvent): Promise<void> {
    const { correlationId, customerId } = event;
    const { planId } = event.payload;

    this.logger.log(
      `Processing ActivationRequested for correlationId: ${correlationId}, customerId: ${customerId}`,
    );

    // Fallo de negocio simulado: no es un error técnico. No se crea la cuenta y se
    // publica BillingFailed como resultado de negocio en billing.events.
    if (event.simulateFailure === 'billing') {
      const reason = 'Fallo simulado en billing';
      this.logger.warn(
        `Simulated billing failure for activation ${correlationId}`,
      );
      await this.kafkaProducer.publishBillingFailed(
        correlationId,
        customerId,
        reason,
        planId,
      );
      this.logger.log(
        `Published BillingFailed for correlationId: ${correlationId}`,
      );
      return;
    }

    const accountId = `acc-${uuidv4().slice(0, 8)}`;

    await this.billingAccountModel.create({
      activationId: correlationId,
      customerId,
      planId,
      accountId,
      status: 'CREATED',
    });
    this.logger.log(
      `Created billing account ${accountId} for activation ${correlationId}`,
    );

    await this.kafkaProducer.publishBillingAccountCreated(
      correlationId,
      customerId,
      accountId,
      planId,
    );
    this.logger.log(
      `Published BillingAccountCreated for correlationId: ${correlationId}`,
    );

    // Caso borde: el ActivationFailed pudo llegar antes (ver handleActivationFailed).
    // Si el marcador ya existe, la cuenta recién creada debe anularse enseguida.
    const failedMarker = await this.failedActivationModel
      .findById(correlationId)
      .lean()
      .exec();
    if (failedMarker) {
      this.logger.warn(
        `Activation ${correlationId} already failed before account creation; cancelling now`,
      );
      await this.cancelAccount(correlationId, customerId, failedMarker.reason);
    }
  }

  async handleActivationFailed(event: ActivationFailedEvent): Promise<void> {
    const { correlationId, customerId } = event;
    const reason = event.payload.reason;

    this.logger.warn(
      `Received ActivationFailed for correlationId: ${correlationId}, failedStep: ${event.payload.failedStep}`,
    );

    // Marcador atómico (upsert): queda registro del fallo aunque la cuenta todavía no exista.
    // Se escribe ANTES de intentar la cancelación para cerrar la carrera con la creación.
    await this.failedActivationModel
      .updateOne(
        { _id: correlationId },
        { $setOnInsert: { reason, at: new Date().toISOString() } },
        { upsert: true },
      )
      .exec();

    // Cancelación atómica: solo si la cuenta existe y sigue en CREATED.
    // Quien gana el findOneAndUpdate (este handler o el post-chequeo de la creación)
    // es el único que publica BillingAccountCancelled: no hay doble publicación.
    await this.cancelAccount(correlationId, customerId, reason);
  }

  private async cancelAccount(
    activationId: string,
    customerId: string,
    reason: string,
  ): Promise<void> {
    const cancelled = await this.billingAccountModel
      .findOneAndUpdate(
        { activationId, status: 'CREATED' },
        {
          $set: {
            status: 'CANCELLED',
            cancelReason: reason,
            updatedAt: new Date(),
          },
        },
        { new: true },
      )
      .lean()
      .exec();

    if (cancelled) {
      this.logger.log(
        `Cancelled billing account ${cancelled.accountId} for activation ${activationId}`,
      );
      await this.kafkaProducer.publishBillingAccountCancelled(
        activationId,
        customerId,
        cancelled.accountId,
        reason,
      );
    } else {
      // La cuenta aún no existe (llegó antes el ActivationRequested) o ya está cancelada
      // (replay o el otro handler ganó la carrera atómica). En ambos casos, nada que publicar.
      this.logger.log(
        `No CREATED account found for activation ${activationId}; cancellation recorded via marker`,
      );
    }
  }
}
