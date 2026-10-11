import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ActivationsService } from './activations.service';
import { EVENT_TYPES, TOPICS } from '@activation-poc/contracts';
import type {
  BillingAccountCreatedEvent,
  BillingFailedEvent,
  ProvisioningCompletedEvent,
  ProvisioningFailedEvent,
} from '@activation-poc/contracts';

@Controller()
export class ActivationEventsController {
  constructor(private readonly activationsService: ActivationsService) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvent(
    @Payload() event: BillingAccountCreatedEvent | BillingFailedEvent,
  ): Promise<void> {
    if (event.eventType === EVENT_TYPES.BILLING_ACCOUNT_CREATED) {
      return this.activationsService.handleBillingAccountCreated(event as BillingAccountCreatedEvent);
    }
    if (event.eventType === EVENT_TYPES.BILLING_FAILED) {
      return this.activationsService.handleBillingFailed(event as BillingFailedEvent);
    }
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvent(
    @Payload() event: ProvisioningCompletedEvent | ProvisioningFailedEvent,
  ): Promise<void> {
    if (event.eventType === EVENT_TYPES.PROVISIONING_COMPLETED) {
      return this.activationsService.handleProvisioningCompleted(event as ProvisioningCompletedEvent);
    }
    if (event.eventType === EVENT_TYPES.PROVISIONING_FAILED) {
      return this.activationsService.handleProvisioningFailed(event as ProvisioningFailedEvent);
    }
  }
}
