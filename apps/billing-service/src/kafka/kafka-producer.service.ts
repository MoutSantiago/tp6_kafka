import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import { ConfigService } from '@nestjs/config';
import { EventEnvelope, TOPICS, createEvent, EVENT_TYPES } from '@activation-poc/contracts';

/**
 * Productor Kafka de billing-service: publica los eventos BillingAccountCreated,
 * BillingFailed y BillingAccountCancelled en el topic billing.events.
 */
@Injectable()
export class KafkaProducerService implements OnModuleInit {
  constructor(
    @Inject('KAFKA_CLIENT') private readonly client: ClientKafka,
    private readonly configService: ConfigService,
  ) {}

  /** Devuelve la configuración de Kafka (brokers y groupId) desde el ConfigService. */
  get kafkaConfig() {
    return {
      brokers: this.configService.get<string[]>('kafka.brokers'),
      groupId: this.configService.get<string>('kafka.groupId'),
    };
  }

  /** Conecta el cliente Kafka al iniciarse el módulo. */
  async onModuleInit(): Promise<void> {
    await this.client.connect();
  }

  /**
   * Publica BillingAccountCreated en billing.events.
   * @param correlationId ID de la activación.
   * @param customerId ID del cliente.
   * @param accountId ID de la cuenta de facturación creada.
   * @param planId Plan asociado.
   */
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

  /**
   * Publica BillingFailed en billing.events.
   * @param correlationId ID de la activación.
   * @param customerId ID del cliente.
   * @param reason Motivo del fallo de facturación.
   * @param planId Plan asociado.
   */
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

  /**
   * Publica BillingAccountCancelled en billing.events (evento de compensación).
   * @param correlationId ID de la activación.
   * @param customerId ID del cliente.
   * @param accountId ID de la cuenta cancelada.
   * @param reason Motivo de la cancelación.
   */
  async publishBillingAccountCancelled(
    correlationId: string,
    customerId: string,
    accountId: string,
    reason: string,
  ): Promise<void> {
    const event = createEvent<{ accountId: string; reason: string }>(
      EVENT_TYPES.BILLING_ACCOUNT_CANCELLED,
      { accountId, reason },
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
          eventType: EVENT_TYPES.BILLING_ACCOUNT_CANCELLED,
        },
      }),
    );
  }

  /**
   * Publica un sobre de evento genérico en un topic arbitrario.
   * @param topic Topic de destino.
   * @param key Clave de partición.
   * @param event Evento a publicar.
   */
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