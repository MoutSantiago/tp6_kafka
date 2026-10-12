export const KAFKA_TOOLKIT_VERSION = '1.0.0';

export function getKafkaToolkitVersion(): string {
  return KAFKA_TOOLKIT_VERSION;
}

export * from './idempotency';
export * from './dlq';
export * from './schemas/processed-event.schema';