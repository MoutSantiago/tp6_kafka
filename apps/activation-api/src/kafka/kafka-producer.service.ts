import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import { EventEnvelope, TOPICS, createEvent, EVENT_TYPES } from '@activation-poc/contracts';

@Injectable()
export class KafkaProducerService implements OnModuleInit {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}

  async onModuleInit(): Promise<void> {
    await this.client.connect();
  }

  async publishActivationRequested(
    correlationId: string,
    customerId: string,
    planId: string,
    simulateFailure: 'none' | 'billing' | 'provisioning' = 'none',
  ): Promise<void> {
    const event = createEvent<{ planId: string; channel: string }>(
      EVENT_TYPES.ACTIVATION_REQUESTED,
      { planId, channel: 'web' },
      {
        correlationId,
        customerId,
        source: 'activation-api',
        simulateFailure,
      },
    );

    await firstValueFrom(
      this.client.emit(TOPICS.ACTIVATION_REQUESTED, {
        key: customerId,
        value: event,
        headers: {
          correlationId,
          eventType: EVENT_TYPES.ACTIVATION_REQUESTED,
        },
      }),
    );
  }

  async publishActivationCompleted(
    correlationId: string,
    customerId: string,
    planId: string,
  ): Promise<void> {
    const event = createEvent<{ planId: string }>(
      EVENT_TYPES.ACTIVATION_COMPLETED,
      { planId },
      {
        correlationId,
        customerId,
        source: 'activation-api',
      },
    );

    await firstValueFrom(
      this.client.emit(TOPICS.ACTIVATION_EVENTS, {
        key: customerId,
        value: event,
        headers: {
          correlationId,
          eventType: EVENT_TYPES.ACTIVATION_COMPLETED,
        },
      }),
    );
  }

  async publishActivationFailed(
    correlationId: string,
    customerId: string,
    planId: string,
    failedStep: 'billing' | 'provisioning',
    reason: string,
  ): Promise<void> {
    const event = createEvent<{
      planId: string;
      failedStep: 'billing' | 'provisioning';
      reason: string;
    }>(
      EVENT_TYPES.ACTIVATION_FAILED,
      { planId, failedStep, reason },
      {
        correlationId,
        customerId,
        source: 'activation-api',
      },
    );

    await firstValueFrom(
      this.client.emit(TOPICS.ACTIVATION_EVENTS, {
        key: customerId,
        value: event,
        headers: {
          correlationId,
          eventType: EVENT_TYPES.ACTIVATION_FAILED,
        },
      }),
    );
  }

  async publish<T extends EventEnvelope<unknown>>(
    topic: string,
    key: string,
    event: T,
  ): Promise<void> {
    await firstValueFrom(
      this.client.emit(topic, {
        key,
        value: event,
        headers: {
          correlationId: event.correlationId,
          eventType: event.eventType,
        },
      }),
    );
  }
}