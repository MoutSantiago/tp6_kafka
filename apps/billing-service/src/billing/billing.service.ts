import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { ClientSession, Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { BillingAccount, BillingAccountDocument } from './billing-account.schema';
import { FailedActivation, FailedActivationDocument } from './failed-activation.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { consumeIdempotently, ProcessedEvent, ProcessedEventDocument } from '@activation-poc/kafka-toolkit';
import type { ActivationRequestedEvent, ActivationFailedEvent } from '@activation-poc/contracts';

/**
 * Servicio de negocio de billing-service. Consume ActivationRequested (crea la
 * cuenta de facturación de forma idempotente o publica BillingFailed si la falla
 * es simulada) y ActivationFailed (compensa cancelando la cuenta, con marcador
 * atómico para el caso borde en que el fallo llega antes que la creación).
 */
@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  constructor(
    @InjectModel(BillingAccount.name)
    private readonly billingAccountModel: Model<BillingAccountDocument>,
    @InjectModel(FailedActivation.name)
    private readonly failedActivationModel: Model<FailedActivationDocument>,
    @InjectModel(ProcessedEvent.name)
    private readonly processedEventModel: Model<ProcessedEventDocument>,
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  /**
   * Procesa ActivationRequested: crea la billing account de forma idempotente
   * (transacción con processed_events) y publica BillingAccountCreated, o
   * BillingFailed si el fallo de billing está simulado.
   * @param event Evento ActivationRequested recibido de Kafka.
   */
  async handleActivationRequested(event: ActivationRequestedEvent): Promise<void> {
    const { correlationId, customerId } = event;
    const { planId } = event.payload;

    this.logger.log(
      `Processing ActivationRequested for correlationId: ${correlationId}, customerId: ${customerId}`,
    );

    const accountId = `acc-${uuidv4().slice(0, 8)}`;

    // Idempotencia: el insert en processed_events y la creación de la cuenta van
    // en la misma transacción. Una entrega duplicada del mismo eventId falla con
    // E11000 y se descarta sin crear una segunda cuenta ni republicar el evento.
    const dup = await consumeIdempotently(
      this.processedEventModel,
      event.eventId,
      async (session) => {
        // Fallo de negocio simulado: no es un error técnico. No se crea la cuenta
        // y se publica BillingFailed como resultado en billing.events.
        if (event.simulateFailure === 'billing') {
          return { ok: false as const, reason: 'Fallo simulado en billing' };
        }
        await this.billingAccountModel.create(
          [
            {
              activationId: correlationId,
              customerId,
              planId,
              accountId,
              status: 'CREATED',
            },
          ],
          { session },
        );
        return { ok: true as const };
      },
    );

    if (dup.result === 'duplicate') {
      this.logger.warn(
        `Duplicate ActivationRequested ${event.eventId} for ${correlationId}, skipping`,
      );
      return;
    }

    const outcome = dup.value!;
    if (!outcome.ok) {
      this.logger.warn(
        `Simulated billing failure for activation ${correlationId}`,
      );
      await this.kafkaProducer.publishBillingFailed(
        correlationId,
        customerId,
        outcome.reason,
        planId,
      );
      this.logger.log(
        `Published BillingFailed for correlationId: ${correlationId}`,
      );
      return;
    }

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
      await this.cancelAccount(correlationId, customerId, undefined, failedMarker.reason);
    }
  }

  /**
   * Procesa ActivationFailed (compensación): registra un marcador de fallo y
   * cancela la cuenta si existe, todo dentro de una transacción idempotente.
   * @param event Evento ActivationFailed recibido de Kafka.
   */
  async handleActivationFailed(event: ActivationFailedEvent): Promise<void> {
    const { correlationId, customerId } = event;
    const reason = event.payload.reason;

    this.logger.warn(
      `Received ActivationFailed for correlationId: ${correlationId}, failedStep: ${event.payload.failedStep}`,
    );

    // Marcador atómico (upsert): queda registro del fallo aunque la cuenta todavía
    // no exista. Marcador + cancelación van en una transacción junto al eventId en
    // processed_events: una entrega duplicada del ActivationFailed se descarta sin
    // efecto (no se republica BillingAccountCancelled).
    await consumeIdempotently(
      this.processedEventModel,
      event.eventId,
      async (session) => {
        await this.failedActivationModel
          .updateOne(
            { _id: correlationId },
            { $setOnInsert: { reason, at: new Date().toISOString() } },
            { upsert: true, session },
          )
          .exec();

        // Cancelación atómica: solo si la cuenta existe y sigue en CREATED.
        // Quien gana el findOneAndUpdate (este handler o el post-chequeo de la
        // creación) es el único que publica BillingAccountCancelled.
        await this.cancelAccount(correlationId, customerId, session, reason);
      },
    );
  }

  /**
   * Cancela atómicamente la cuenta de una activación (solo si sigue en CREATED)
   * y publica BillingAccountCancelled. Si la cuenta no existe o ya se canceló,
   * no publica nada (deduplicación entre handlers concurrentes).
   * @param activationId ID de la activación.
   * @param customerId ID del cliente.
   * @param session Sesión de la transacción Mongo en curso, si aplica.
   * @param reason Motivo de la cancelación.
   */
  private async cancelAccount(
    activationId: string,
    customerId: string,
    session: ClientSession | undefined,
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
        { new: true, session },
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
