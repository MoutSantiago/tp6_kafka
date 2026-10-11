import { Inject, Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { createTransport, Transporter } from 'nodemailer';
import { Notification } from './notification.schema';
import type { NotificationDocument } from './notification.schema';
import { smtpConfig } from '../config/env.config';
import type { SmtpConfig } from '../config/env.config';
import { EVENT_TYPES } from '@activation-poc/contracts';
import type { ActivationCompletedEvent, ActivationFailedEvent } from '@activation-poc/contracts';

type ActivationEvent = ActivationCompletedEvent | ActivationFailedEvent;

@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);
  private readonly transporter: Transporter;
  private readonly from: string;

  constructor(
    @InjectModel(Notification.name)
    private readonly notificationModel: Model<NotificationDocument>,
    @Inject(smtpConfig.KEY) config: SmtpConfig,
  ) {
    this.transporter = createTransport({
      host: config.host,
      port: config.port,
      secure: false,
    });
    this.from = config.from;
  }

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

    // Los errores técnicos del SMTP (nodemailer.sendMail rechazado) se propagan:
    // Nest no commitea el offset y el mensaje se reintenta (fase 3 refinará con
    // reintentos acotados y DLQ). Solo los emails efectivamente enviados quedan
    // registrados en `notifications`.
    await this.transporter.sendMail({
      from: this.from,
      to,
      subject,
      text,
    });

    await this.notificationModel.create({
      activationId: correlationId,
      eventType,
      customerId,
      to,
      subject,
      sentAt: new Date(),
    });

    this.logger.log(`Sent "${subject}" to ${to} (activation ${correlationId})`);
  }

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
