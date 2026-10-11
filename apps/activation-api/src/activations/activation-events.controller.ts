import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ActivationsService } from './activations.service';
import { TOPICS } from '@activation-poc/contracts';
import type {
  BillingAccountCreatedEvent,
  ProvisioningCompletedEvent,
} from '@activation-poc/contracts';

@Controller()
export class ActivationEventsController {
  constructor(private readonly activationsService: ActivationsService) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingAccountCreated(
    @Payload() event: BillingAccountCreatedEvent,
  ): Promise<void> {
    return this.activationsService.handleBillingAccountCreated(event);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningCompleted(
    @Payload() event: ProvisioningCompletedEvent,
  ): Promise<void> {
    return this.activationsService.handleProvisioningCompleted(event);
  }
}
