import { v4 as uuidv4 } from 'uuid';

/** Etapa en la que se simula un fallo de negocio (o 'none' si la saga va OK). */
export type SimulateFailure = 'none' | 'billing' | 'provisioning';

/** Estados posibles de la saga de activación. */
export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

/**
 * Sobre estándar de todos los eventos de negocio de la POC: metadatos de
 * correlación, trazabilidad y el payload tipado.
 */
export interface EventEnvelope<TPayload = unknown> {
  eventId: string;
  eventType: string;
  version: number;
  occurredAt: string;
  correlationId: string;
  customerId: string;
  source: string;
  simulateFailure?: SimulateFailure;
  payload: TPayload;
}

/** Payload del evento ActivationRequested. */
export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
}

/** Payload del evento BillingAccountCreated. */
export interface BillingAccountCreatedPayload {
  accountId: string;
  planId: string;
}

/** Payload del evento BillingFailed. */
export interface BillingFailedPayload {
  reason: string;
  planId: string;
}

/** Payload del evento BillingAccountCancelled (compensación). */
export interface BillingAccountCancelledPayload {
  accountId: string;
  reason: string;
}

/** Payload del evento ProvisioningCompleted. */
export interface ProvisioningCompletedPayload {
  orderId: string;
  lineId: string;
}

/** Payload del evento ProvisioningFailed. */
export interface ProvisioningFailedPayload {
  orderId: string;
  reason: string;
}

/** Payload del evento ActivationCompleted. */
export interface ActivationCompletedPayload {
  planId: string;
}

/** Payload del evento ActivationFailed. */
export interface ActivationFailedPayload {
  planId: string;
  failedStep: 'billing' | 'provisioning';
  reason: string;
}

/** Aliases de eventos concretos: sobre + payload de cada tipo de evento. */
export type ActivationRequestedEvent = EventEnvelope<ActivationRequestedPayload>;
export type BillingAccountCreatedEvent = EventEnvelope<BillingAccountCreatedPayload>;
export type BillingFailedEvent = EventEnvelope<BillingFailedPayload>;
export type BillingAccountCancelledEvent = EventEnvelope<BillingAccountCancelledPayload>;
export type ProvisioningCompletedEvent = EventEnvelope<ProvisioningCompletedPayload>;
export type ProvisioningFailedEvent = EventEnvelope<ProvisioningFailedPayload>;
export type ActivationCompletedEvent = EventEnvelope<ActivationCompletedPayload>;
export type ActivationFailedEvent = EventEnvelope<ActivationFailedPayload>;

/** Unión de todos los eventos de negocio de la POC. */
export type AnyEvent =
  | ActivationRequestedEvent
  | BillingAccountCreatedEvent
  | BillingFailedEvent
  | BillingAccountCancelledEvent
  | ProvisioningCompletedEvent
  | ProvisioningFailedEvent
  | ActivationCompletedEvent
  | ActivationFailedEvent;

/** Versión actual del esquema de eventos. */
export const EVENT_VERSION = 1;

/** Topics de negocio de la POC. */
export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

/** Topics de dead-letter queue asociados a cada topic de negocio. */
export const DLQ_TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested.dlq',
  BILLING_EVENTS: 'billing.events.dlq',
  PROVISIONING_EVENTS: 'provisioning.events.dlq',
  ACTIVATION_EVENTS: 'activation.events.dlq',
} as const;

/** Lista completa de topics (negocio + DLQ) que debe existir en el broker. */
export const ALL_TOPICS = [
  TOPICS.ACTIVATION_REQUESTED,
  TOPICS.BILLING_EVENTS,
  TOPICS.PROVISIONING_EVENTS,
  TOPICS.ACTIVATION_EVENTS,
  DLQ_TOPICS.ACTIVATION_REQUESTED,
  DLQ_TOPICS.BILLING_EVENTS,
  DLQ_TOPICS.PROVISIONING_EVENTS,
  DLQ_TOPICS.ACTIVATION_EVENTS,
] as const;

/** Grupos de consumidores Kafka de cada servicio. */
export const CONSUMER_GROUPS = {
  BILLING_SVC: 'billing-svc',
  PROVISIONING_SVC: 'provisioning-svc',
  CRM_ANALYTICS: 'crm-analytics',
  ACTIVATION_API: 'activation-api',
  NOTIFICATION_SVC: 'notification-svc',
  LOYALTY_SVC: 'loyalty-svc',
} as const;

/** Nombres de los tipos de evento de negocio. */
export const EVENT_TYPES = {
  ACTIVATION_REQUESTED: 'ActivationRequested',
  BILLING_ACCOUNT_CREATED: 'BillingAccountCreated',
  BILLING_FAILED: 'BillingFailed',
  BILLING_ACCOUNT_CANCELLED: 'BillingAccountCancelled',
  PROVISIONING_COMPLETED: 'ProvisioningCompleted',
  PROVISIONING_FAILED: 'ProvisioningFailed',
  ACTIVATION_COMPLETED: 'ActivationCompleted',
  ACTIVATION_FAILED: 'ActivationFailed',
} as const;

/**
 * Crea un sobre de evento estándar con eventId nuevo, fecha de ocurrencia y
 * metadatos de correlación.
 * @param eventType Tipo del evento (ver EVENT_TYPES).
 * @param payload Payload del evento.
 * @param options Metadatos: correlationId, customerId, source y opcionales
 * versión y simulateFailure.
 * @returns Sobre de evento listo para publicar.
 */
export function createEvent<TPayload>(
  eventType: string,
  payload: TPayload,
  options: {
    correlationId: string;
    customerId: string;
    source: string;
    version?: number;
    simulateFailure?: SimulateFailure;
  }
): EventEnvelope<TPayload> {
  return {
    eventId: uuidv4(),
    eventType,
    version: options.version ?? EVENT_VERSION,
    occurredAt: new Date().toISOString(),
    correlationId: options.correlationId,
    customerId: options.customerId,
    source: options.source,
    simulateFailure: options.simulateFailure,
    payload,
  };
}