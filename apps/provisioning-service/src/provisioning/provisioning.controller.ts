import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { ProvisioningService } from './provisioning.service';
import { TOPICS } from '@activation-poc/contracts';
import type { ActivationRequestedEvent } from '@activation-poc/contracts';

@Controller()
export class ProvisioningController {
  constructor(private readonly provisioningService: ProvisioningService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: ActivationRequestedEvent,
  ): Promise<void> {
    return this.provisioningService.handleActivationRequested(event);
  }
}
