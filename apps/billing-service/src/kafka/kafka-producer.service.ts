import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import { ConfigService } from '@nestjs/config';
import { EventEnvelope, TOPICS, createEvent, EVENT_TYPES } from '@activation-poc/contracts';

@Injectable()
export class KafkaProducerService implements OnModuleInit {
  constructor(
    @Inject('KAFKA_CLIENT') private readonly client: ClientKafka,
    private readonly configService: ConfigService,
  ) {}

  get kafkaConfig() {
    return {
      brokers: this.configService.get<string[]>('kafka.brokers'),
      groupId: this.configService.get<string>('kafka.groupId'),
    };
  }

  async onModuleInit(): Promise<void> {
    await this.client.connect();
  }

  async publishBillingAccountCreated(
    correlationId: string,
    customerId: string,
    accountId: string,
    planId: string,
  ): Promise<void> {
    const event = createEvent<{ accountId: string; planId: string }>(
      EVENT_TYPES.BILLING_ACCOUNT_CREATED,
      { accountId, planId },
      {
        correlationId,
        customerId,
        source: 'billing-service',
      },
    );

    await firstValueFrom(
      this.client.emit(TOPICS.BILLING_EVENTS, {
        key: customerId,
        value: event,
        headers: {
          correlationId,
          eventType: EVENT_TYPES.BILLING_ACCOUNT_CREATED,
        },
      }),
    );
  }

  async publishBillingFailed(
    correlationId: string,
    customerId: string,
    reason: string,
    planId: string,
  ): Promise<void> {
    const event = createEvent<{ reason: string; planId: string }>(
      EVENT_TYPES.BILLING_FAILED,
      { reason, planId },
      {
        correlationId,
        customerId,
        source: 'billing-service',
      },
    );

    await firstValueFrom(
      this.client.emit(TOPICS.BILLING_EVENTS, {
        key: customerId,
        value: event,
        headers: {
          correlationId,
          eventType: EVENT_TYPES.BILLING_FAILED,
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