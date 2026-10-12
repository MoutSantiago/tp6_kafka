import { Controller } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { NotificationService } from './notification.service';
import { TOPICS } from '@activation-poc/contracts';
import {
  assertEventEnvelope,
  DlqHandler,
  toRawKafkaMessage,
} from '@activation-poc/kafka-toolkit';
import type {
  ActivationCompletedEvent,
  ActivationFailedEvent,
} from '@activation-poc/contracts';

@Controller()
export class NotificationController {
  private readonly dlq: DlqHandler;

  constructor(
    private readonly notificationService: NotificationService,
    config: ConfigService,
  ) {
    this.dlq = new DlqHandler(
      config.getOrThrow<string[]>('kafka.brokers'),
      'notification-service-dlq',
    );
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvent(
    @Payload() event: ActivationCompletedEvent | ActivationFailedEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.dlq.run(
      toRawKafkaMessage({ topic: context.getTopic(), ...context.getMessage() }),
      async () => {
        assertEventEnvelope(event);
        await this.notificationService.handleActivationEvent(event);
      },
    );
  }
}
