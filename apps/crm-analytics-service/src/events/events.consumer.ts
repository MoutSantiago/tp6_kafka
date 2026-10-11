import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { EventsService } from './events.service';
import { TOPICS } from '@activation-poc/contracts';
import type { AnyEvent } from '@activation-poc/contracts';

@Controller()
export class EventsConsumer {
  constructor(private readonly eventsService: EventsService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.eventsService.recordEvent(event, context.getTopic());
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.eventsService.recordEvent(event, context.getTopic());
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.eventsService.recordEvent(event, context.getTopic());
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(
    @Payload() event: AnyEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    return this.eventsService.recordEvent(event, context.getTopic());
  }
}
