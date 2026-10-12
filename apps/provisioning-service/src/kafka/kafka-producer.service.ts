import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import { EventEnvelope, TOPICS, createEvent, EVENT_TYPES } from '@activation-poc/contracts';

/**
 * Productor Kafka de provisioning-service: publica los eventos
 * ProvisioningCompleted y ProvisioningFailed en el topic provisioning.events.
 */
@Injectable()
export class KafkaProducerService implements OnModuleInit {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}

  /** Conecta el cliente Kafka al iniciarse el módulo. */
  async onModuleInit(): Promise<void> {
    await this.client.connect();
  }

  /**
   * Publica ProvisioningCompleted en provisioning.events.
   * @param correlationId ID de la activación.
   * @param customerId ID del cliente.
   * @param orderId ID de la orden creada.
   * @param lineId ID de la línea aprovisionada.
   */
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

  /**
   * Publica ProvisioningFailed en provisioning.events.
   * @param correlationId ID de la activación.
   * @param customerId ID del cliente.
   * @param orderId ID de la orden.
   * @param reason Motivo del fallo.
   */
  async publishProvisioningFailed(
    correlationId: string,
    customerId: string,
    orderId: string,
    reason: string,
  ): Promise<void> {
    const event = createEvent<{ orderId: string; reason: string }>(
      EVENT_TYPES.PROVISIONING_FAILED,
      { orderId, reason },
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
          eventType: EVENT_TYPES.PROVISIONING_FAILED,
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
