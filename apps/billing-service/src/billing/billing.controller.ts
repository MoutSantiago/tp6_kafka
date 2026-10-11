import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { BillingService } from './billing.service';
import { EVENT_TYPES, TOPICS } from '@activation-poc/contracts';
import type {
  ActivationRequestedEvent,
  ActivationFailedEvent,
} from '@activation-poc/contracts';

@Controller()
export class BillingController {
  constructor(private readonly billingService: BillingService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: ActivationRequestedEvent,
  ): Promise<void> {
    return this.billingService.handleActivationRequested(event);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvent(
    @Payload() event: ActivationFailedEvent,
  ): Promise<void> {
    if (event.eventType === EVENT_TYPES.ACTIVATION_FAILED) {
      return this.billingService.handleActivationFailed(event);
    }
  }
}
