import { Controller } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { BillingService } from './billing.service';
import { EVENT_TYPES, TOPICS } from '@activation-poc/contracts';
import {
  assertEventEnvelope,
  DlqHandler,
  toRawKafkaMessage,
} from '@activation-poc/kafka-toolkit';
import type {
  ActivationRequestedEvent,
  ActivationFailedEvent,
} from '@activation-poc/contracts';

/**
 * Controlador-consumidor Kafka de billing-service: escucha activation.requested
 * y activation.events, valida los sobres y delega en BillingService envolviendo
 * todo con protección DLQ.
 */
@Controller()
export class BillingController {
  private readonly dlq: DlqHandler;

  constructor(
    private readonly billingService: BillingService,
    config: ConfigService,
  ) {
    this.dlq = new DlqHandler(
      config.getOrThrow<string[]>('kafka.brokers'),
      'billing-service-dlq',
    );
  }

  /**
   * Escucha el topic activation.requested y delega en el servicio con DLQ.
   * @param event Evento ActivationRequested.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: ActivationRequestedEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.dlq.run(
      this.rawMessage(context),
      async () => {
        assertEventEnvelope(event);
        await this.billingService.handleActivationRequested(event);
      },
    );
  }

  /**
   * Escucha el topic activation.events y procesa solo los ActivationFailed.
   * @param event Evento recibido (se verifica que sea ActivationFailed).
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvent(
    @Payload() event: ActivationFailedEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.dlq.run(
      this.rawMessage(context),
      async () => {
        assertEventEnvelope(event);
        if (event.eventType === EVENT_TYPES.ACTIVATION_FAILED) {
          await this.billingService.handleActivationFailed(event);
        }
      },
    );
  }

  /**
   * Arma el mensaje Kafka crudo (para el DLQ) a partir del contexto Nest.
   * @param context Contexto Kafka del mensaje recibido.
   * @returns Mensaje crudo listo para re-publicar en el DLQ.
   */
  private rawMessage(context: KafkaContext) {
    return toRawKafkaMessage({
      topic: context.getTopic(),
      ...context.getMessage(),
    });
  }
}
