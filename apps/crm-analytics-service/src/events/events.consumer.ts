import { Controller } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { EventsService } from './events.service';
import { TOPICS } from '@activation-poc/contracts';
import {
  assertEventEnvelope,
  DlqHandler,
  toRawKafkaMessage,
} from '@activation-poc/kafka-toolkit';
import type { AnyEvent } from '@activation-poc/contracts';

/**
 * Consumidor Kafka de crm-analytics-service: escucha todos los topics de
 * negocio (activation.requested, billing.events, provisioning.events y
 * activation.events) y registra cada evento con protección DLQ.
 */
@Controller()
export class EventsConsumer {
  private readonly dlq: DlqHandler;

  constructor(
    private readonly eventsService: EventsService,
    config: ConfigService,
  ) {
    this.dlq = new DlqHandler(
      config.getOrThrow<string[]>('kafka.brokers'),
      'crm-analytics-dlq',
    );
  }

  /**
   * Escucha el topic activation.requested y registra el evento.
   * @param event Evento recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.safeRecord(event, context);
  }

  /**
   * Escucha el topic billing.events y registra el evento.
   * @param event Evento recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.safeRecord(event, context);
  }

  /**
   * Escucha el topic provisioning.events y registra el evento.
   * @param event Evento recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.safeRecord(event, context);
  }

  /**
   * Escucha el topic activation.events y registra el evento.
   * @param event Evento recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.safeRecord(event, context);
  }

  /**
   * Valida el sobre del evento y lo registra en Mongo envolviendo el
   * procesamiento con el manejador de DLQ.
   * @param event Evento recibido de cualquier topic de negocio.
   * @param context Contexto Kafka del mensaje.
   */
  private async safeRecord(event: AnyEvent, context: KafkaContext): Promise<void> {
    await this.dlq.run(
      toRawKafkaMessage({ topic: context.getTopic(), ...context.getMessage() }),
      async () => {
        assertEventEnvelope(event);
        await this.eventsService.recordEvent(event, context.getTopic());
      },
    );
  }
}
