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

  private rawMessage(context: KafkaContext) {
    return toRawKafkaMessage({
      topic: context.getTopic(),
      ...context.getMessage(),
    });
  }
}
