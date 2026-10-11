import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { NotificationService } from './notification.service';
import { TOPICS } from '@activation-poc/contracts';
import type {
  ActivationCompletedEvent,
  ActivationFailedEvent,
} from '@activation-poc/contracts';

@Controller()
export class NotificationController {
  constructor(private readonly notificationService: NotificationService) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvent(
    @Payload() event: ActivationCompletedEvent | ActivationFailedEvent,
  ): Promise<void> {
    return this.notificationService.handleActivationEvent(event);
  }
}
