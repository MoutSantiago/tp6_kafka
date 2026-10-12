import { Controller } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { ActivationsService } from './activations.service';
import { EVENT_TYPES, TOPICS } from '@activation-poc/contracts';
import {
  assertEventEnvelope,
  DlqHandler,
  toRawKafkaMessage,
} from '@activation-poc/kafka-toolkit';
import type {
  BillingAccountCreatedEvent,
  BillingAccountCancelledEvent,
  BillingFailedEvent,
  ProvisioningCompletedEvent,
  ProvisioningFailedEvent,
} from '@activation-poc/contracts';

/**
 * Controlador-consumidor Kafka de activation-api: escucha los topics
 * billing.events y provisioning.events y delega cada evento al ActivationsService.
 * Todo el procesamiento corre dentro del wrapper de DLQ del kafka-toolkit.
 */
@Controller()
export class ActivationEventsController {
  private readonly dlq: DlqHandler;

  constructor(
    private readonly activationsService: ActivationsService,
    config: ConfigService,
  ) {
    this.dlq = new DlqHandler(
      config.getOrThrow<string[]>('kafka.brokers'),
      'activation-api-dlq',
    );
  }

  /**
   * Procesa eventos del topic billing.events (BillingAccountCreated,
   * BillingFailed o BillingAccountCancelled) con protección DLQ.
   * @param event Evento de facturación recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvent(
    @Payload() event: BillingAccountCreatedEvent | BillingFailedEvent | BillingAccountCancelledEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.dlq.run(
      toRawKafkaMessage({ topic: context.getTopic(), ...context.getMessage() }),
      async () => {
        assertEventEnvelope(event);
        if (event.eventType === EVENT_TYPES.BILLING_ACCOUNT_CREATED) {
          await this.activationsService.handleBillingAccountCreated(event as BillingAccountCreatedEvent);
        } else if (event.eventType === EVENT_TYPES.BILLING_FAILED) {
          await this.activationsService.handleBillingFailed(event as BillingFailedEvent);
        } else if (event.eventType === EVENT_TYPES.BILLING_ACCOUNT_CANCELLED) {
          await this.activationsService.handleBillingAccountCancelled(event as BillingAccountCancelledEvent);
        }
      },
    );
  }

  /**
   * Procesa eventos del topic provisioning.events (ProvisioningCompleted
   * o ProvisioningFailed) con protección DLQ.
   * @param event Evento de aprovisionamiento recibido.
   * @param context Contexto Kafka (topic, partición, offset).
   */
  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvent(
    @Payload() event: ProvisioningCompletedEvent | ProvisioningFailedEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.dlq.run(
      toRawKafkaMessage({ topic: context.getTopic(), ...context.getMessage() }),
      async () => {
        assertEventEnvelope(event);
        if (event.eventType === EVENT_TYPES.PROVISIONING_COMPLETED) {
          await this.activationsService.handleProvisioningCompleted(event as ProvisioningCompletedEvent);
        } else if (event.eventType === EVENT_TYPES.PROVISIONING_FAILED) {
          await this.activationsService.handleProvisioningFailed(event as ProvisioningFailedEvent);
        }
      },
    );
  }
}
