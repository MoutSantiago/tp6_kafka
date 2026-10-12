/** Versión de la librería kafka-toolkit. */
export const KAFKA_TOOLKIT_VERSION = '1.0.0';

/**
 * Devuelve la versión de kafka-toolkit.
 * @returns Cadena con la versión actual.
 */
export function getKafkaToolkitVersion(): string {
  return KAFKA_TOOLKIT_VERSION;
}

export * from './idempotency';
export * from './dlq';
export * from './schemas/processed-event.schema';