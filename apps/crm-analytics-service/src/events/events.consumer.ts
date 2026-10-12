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

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.safeRecord(event, context);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.safeRecord(event, context);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.safeRecord(event, context);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.safeRecord(event, context);
  }

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
