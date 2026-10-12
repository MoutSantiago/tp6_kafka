import { Inject, Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { createTransport, Transporter } from 'nodemailer';
import { consumeIdempotently, ProcessedEvent } from '@activation-poc/kafka-toolkit';
import type { ProcessedEventDocument } from '@activation-poc/kafka-toolkit';
import { Notification } from './notification.schema';
import type { NotificationDocument } from './notification.schema';
import { smtpConfig } from '../config/env.config';
import type { SmtpConfig } from '../config/env.config';
import { EVENT_TYPES } from '@activation-poc/contracts';
import type { ActivationCompletedEvent, ActivationFailedEvent } from '@activation-poc/contracts';

/** Union de los eventos de activación que generan notificaciones. */
type ActivationEvent = ActivationCompletedEvent | ActivationFailedEvent;

/**
 * Servicio de negocio de notification-service: ante ActivationCompleted o
 * ActivationFailed envía un email simulado (SMTP) y registra la notificación
 * en Mongo, todo de forma idempotente.
 */
@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);
  private readonly transporter: Transporter;
  private readonly from: string;

  constructor(
    @InjectModel(Notification.name)
    private readonly notificationModel: Model<NotificationDocument>,
    @InjectModel(ProcessedEvent.name)
    private readonly processedEventModel: Model<ProcessedEventDocument>,
    @Inject(smtpConfig.KEY) config: SmtpConfig,
  ) {
    this.transporter = createTransport({
      host: config.host,
      port: config.port,
      secure: false,
    });
    this.from = config.from;
  }

  /**
   * Procesa un evento de activación: envía el email correspondiente y registra
   * la notificación dentro de una transacción idempotente (los duplicados no
   * reenvían el email).
   * @param event ActivationCompleted o ActivationFailed recibido de Kafka.
   */
  async handleActivationEvent(event: ActivationEvent): Promise<void> {
    const { correlationId, customerId, eventType } = event;

    if (
      eventType !== EVENT_TYPES.ACTIVATION_COMPLETED &&
      eventType !== EVENT_TYPES.ACTIVATION_FAILED
    ) {
      return;
    }

    this.logger.log(
      `Preparing ${eventType} email for activation ${correlationId}, customer ${customerId}`,
    );

    const { subject, text } = this.buildEmail(event);
    const to = `${customerId}@demo.local`;

    // Idempotencia: el insert del eventId en processed_events y el efecto
    // (envío del email + registro en notifications) van en una transacción.
    // Entregas duplicadas del mismo eventId se descartan sin reenviar el email.
    // Si el SMTP falla técnicamente, sendMail lanza y la transacción aborta:
    // el evento NO queda marcado, Nest reintenta la entrega (fase 3 refinará
    // con reintentos acotados y DLQ).
    const { result } = await consumeIdempotently(
      this.processedEventModel,
      event.eventId,
      async (session) => {
        await this.transporter.sendMail({
          from: this.from,
          to,
          subject,
          text,
        });
        await this.notificationModel.create(
          [
            {
              activationId: correlationId,
              eventType,
              customerId,
              to,
              subject,
              sentAt: new Date(),
            },
          ],
          { session },
        );
      },
    );

    if (result === 'duplicate') {
      this.logger.warn(
        `Duplicate activation event ${event.eventId}, skipping email send`,
      );
      return;
    }

    this.logger.log(`Sent "${subject}" to ${to} (activation ${correlationId})`);
  }

  /**
   * Arma asunto y cuerpo del email según el tipo de evento.
   * @param event Evento de activación.
   * @returns Objeto con subject y text del email.
   */
  private buildEmail(event: ActivationEvent): { subject: string; text: string } {
    if (event.eventType === EVENT_TYPES.ACTIVATION_COMPLETED) {
      return {
        subject: 'Tu activación está lista',
        text: `Hola ${event.customerId},\n\nTu plan ya está activo (activación ${event.correlationId}). ¡Bienvenido!\n`,
      };
    }
    const failed = event as ActivationFailedEvent;
    return {
      subject: 'No pudimos completar tu activación',
      text: `Hola ${event.customerId},\n\nLa activación ${event.correlationId} falló en el paso "${failed.payload.failedStep}": ${failed.payload.reason}.\n`,
    };
  }
}
