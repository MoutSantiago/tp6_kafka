import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import { EventEnvelope, TOPICS, createEvent, EVENT_TYPES } from '@activation-poc/contracts';

/**
 * Productor Kafka de activation-api. Publica los eventos de la saga de activación
 * (ActivationRequested, ActivationCompleted, ActivationFailed) en los topics
 * correspondientes, agregando correlationId y eventType como headers.
 */
@Injectable()
export class KafkaProducerService implements OnModuleInit {
  constructor(@Inject('KAFKA_CLIENT') private readonly client: ClientKafka) {}

  /** Conecta el cliente Kafka al iniciarse el módulo. */
  async onModuleInit(): Promise<void> {
    await this.client.connect();
  }

  /**
   * Publica el evento ActivationRequested en el topic activation.requested.
   * @param correlationId ID de la activación (clave de partición y correlación).
   * @param customerId ID del cliente.
   * @param planId Plan solicitado.
   * @param simulateFailure Etapa que debe fallar en la simulación, si aplica.
   */
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

  /**
   * Publica ActivationCompleted en el topic activation.events cuando la saga termina OK.
   * @param correlationId ID de la activación.
   * @param customerId ID del cliente.
   * @param planId Plan activado.
   */
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

  /**
   * Publica ActivationFailed en el topic activation.events cuando la saga falla.
   * @param correlationId ID de la activación.
   * @param customerId ID del cliente.
   * @param planId Plan solicitado.
   * @param failedStep Etapa que falló ('billing' | 'provisioning').
   * @param reason Motivo del fallo.
   */
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

  /**
   * Publica un sobre de evento genérico en un topic arbitrario.
   * @param topic Topic de destino.
   * @param key Clave de partición (típicamente customerId).
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