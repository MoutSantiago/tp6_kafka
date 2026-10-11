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

  async publishProvisioningCompleted(
    correlationId: string,
    customerId: string,
    orderId: string,
    lineId: string,
  ): Promise<void> {
    const event = createEvent<{ orderId: string; lineId: string }>(
      EVENT_TYPES.PROVISIONING_COMPLETED,
      { orderId, lineId },
      {
        correlationId,
        customerId,
        source: 'provisioning-service',
      },
    );

    await firstValueFrom(
      this.client.emit(TOPICS.PROVISIONING_EVENTS, {
        key: customerId,
        value: event,
        headers: {
          correlationId,
          eventType: EVENT_TYPES.PROVISIONING_COMPLETED,
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
