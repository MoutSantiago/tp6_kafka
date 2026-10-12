import { Controller, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS, CONSUMER_GROUPS } from '@activation-poc/contracts';
import {
  assertEventEnvelope,
  DlqHandler,
  toRawKafkaMessage,
} from '@activation-poc/kafka-toolkit';
import type { AnyEvent } from '@activation-poc/contracts';

/**
 * Consumidor Kafka de loyalty-service: escucha todos los topics de negocio y
 * solo loguea cada evento recibido (demuestra consumo de historial completo),
 * con protección DLQ.
 */
@Controller()
export class LoyaltyController {
  private readonly logger = new Logger(LoyaltyController.name);
  private readonly dlq: DlqHandler;

  constructor(config: ConfigService) {
    this.dlq = new DlqHandler(
      config.getOrThrow<string[]>('kafka.brokers'),
      'loyalty-service-dlq',
    );
    this.logger.log(
      `Loyalty consumer created for group ${CONSUMER_GROUPS.LOYALTY_SVC}`,
    );
  }

  /**
   * Escucha el topic activation.requested y loguea el evento.
   * @param event Evento recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.logEvent(event, context);
  }

  /**
   * Escucha el topic billing.events y loguea el evento.
   * @param event Evento recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.logEvent(event, context);
  }

  /**
   * Escucha el topic provisioning.events y loguea el evento.
   * @param event Evento recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.logEvent(event, context);
  }

  /**
   * Escucha el topic activation.events y loguea el evento.
   * @param event Evento recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.logEvent(event, context);
  }

  /**
   * Valida el sobre del evento y lo loguea (tipo, ID, topic, partición,
   * correlationId y payload) envuelto en el manejador de DLQ.
   * @param event Evento recibido de cualquier topic de negocio.
   * @param context Contexto Kafka del mensaje.
   */
  private async logEvent(event: AnyEvent, context: KafkaContext): Promise<void> {
    await this.dlq.run(
      toRawKafkaMessage({ topic: context.getTopic(), ...context.getMessage() }),
      async () => {
        assertEventEnvelope(event);
        this.logger.log(
          `[loyalty] ${event.eventType} (${event.eventId}) | topic=${context.getTopic()} partition=${context.getPartition()} correlationId=${event.correlationId} customer=${event.customerId} payload=${JSON.stringify(event.payload)}`,
        );
      },
    );
  }
}
