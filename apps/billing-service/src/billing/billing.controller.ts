import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { BillingService } from './billing.service';
import { TOPICS } from '@activation-poc/contracts';
import type { ActivationRequestedEvent } from '@activation-poc/contracts';

@Controller()
export class BillingController {
  constructor(private readonly billingService: BillingService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: ActivationRequestedEvent,
  ): Promise<void> {
    return this.billingService.handleActivationRequested(event);
  }
}
