import { v4 as uuidv4 } from 'uuid';

export type SimulateFailure = 'none' | 'billing' | 'provisioning';

export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

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

export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
}

export interface BillingAccountCreatedPayload {
  accountId: string;
  planId: string;
}

export interface BillingFailedPayload {
  reason: string;
  planId: string;
}

export interface BillingAccountCancelledPayload {
  accountId: string;
  reason: string;
}

export interface ProvisioningCompletedPayload {
  orderId: string;
  lineId: string;
}

export interface ProvisioningFailedPayload {
  orderId: string;
  reason: string;
}

export interface ActivationCompletedPayload {
  planId: string;
}

export interface ActivationFailedPayload {
  planId: string;
  failedStep: 'billing' | 'provisioning';
  reason: string;
}

export type ActivationRequestedEvent = EventEnvelope<ActivationRequestedPayload>;
export type BillingAccountCreatedEvent = EventEnvelope<BillingAccountCreatedPayload>;
export type BillingFailedEvent = EventEnvelope<BillingFailedPayload>;
export type BillingAccountCancelledEvent = EventEnvelope<BillingAccountCancelledPayload>;
export type ProvisioningCompletedEvent = EventEnvelope<ProvisioningCompletedPayload>;
export type ProvisioningFailedEvent = EventEnvelope<ProvisioningFailedPayload>;
export type ActivationCompletedEvent = EventEnvelope<ActivationCompletedPayload>;
export type ActivationFailedEvent = EventEnvelope<ActivationFailedPayload>;

export type AnyEvent =
  | ActivationRequestedEvent
  | BillingAccountCreatedEvent
  | BillingFailedEvent
  | BillingAccountCancelledEvent
  | ProvisioningCompletedEvent
  | ProvisioningFailedEvent
  | ActivationCompletedEvent
  | ActivationFailedEvent;

export const EVENT_VERSION = 1;

export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export const DLQ_TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested.dlq',
  BILLING_EVENTS: 'billing.events.dlq',
  PROVISIONING_EVENTS: 'provisioning.events.dlq',
  ACTIVATION_EVENTS: 'activation.events.dlq',
} as const;

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

export const CONSUMER_GROUPS = {
  BILLING_SVC: 'billing-svc',
  PROVISIONING_SVC: 'provisioning-svc',
  CRM_ANALYTICS: 'crm-analytics',
  ACTIVATION_API: 'activation-api',
  NOTIFICATION_SVC: 'notification-svc',
  LOYALTY_SVC: 'loyalty-svc',
} as const;

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