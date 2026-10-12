import { Logger } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';

/** Cantidad máxima de reintentos antes de enviar un mensaje a la DLQ. */
export const DLQ_MAX_RETRIES = 3;
/** Esperas progresivas entre reintentos: 1s, 2s, 4s. */
export const DLQ_RETRY_DELAYS_MS = [1000, 2000, 4000] as const; // 1s, 2s, 4s
/** Sufijo de los topics dead-letter queue. */
export const DLQ_SUFFIX = '.dlq';

/**
 * Valida que el payload tenga la forma mínima de un envelope de evento.
 * Si el JSON vino roto o le faltan campos, lanza un error TÉCNICO: pasa por
 * los reintentos/DLQ (no es un fallo de negocio).
 */
export function assertEventEnvelope(event: unknown): asserts event is {
  eventId: string;
  eventType: string;
  correlationId: string;
  customerId: string;
  payload: unknown;
} {
  const e = event as Record<string, unknown> | null;
  if (
    e === null ||
    typeof e !== 'object' ||
    typeof e.eventId !== 'string' ||
    typeof e.eventType !== 'string' ||
    typeof e.correlationId !== 'string' ||
    typeof e.customerId !== 'string' ||
    e.payload === undefined
  ) {
    throw new Error(
      `Invalid event envelope: expected object with eventId, eventType, correlationId, customerId and payload`,
    );
  }
}

/** Mensaje original normalizado para republicar en la DLQ tal cual llegó. */
export interface RawKafkaMessage {
  topic: string;
  key: string | Buffer | null;
  value: string;
  headers: Record<string, string>;
}

/**
 * Wrapper de errores técnicos para handlers de consumidor (RNF-04).
 *
 * Ejecuta el handler; si lanza error, reintenta hasta 3 veces con espera
 * creciente (1 s, 2 s, 4 s). Si sigue fallando, publica el mensaje ORIGINAL
 * en `<topic>.dlq` (con `error`, `retryCount`, `originalTopic` y `failedAt` en
 * headers) y devuelve normalmente: el consumidor commitea el offset y la
 * partición NO se bloquea.
 *
 * Los fallos de negocio simulados NO pasan por acá: se publican como
 * BillingFailed / ProvisioningFailed antes (el handler no lanza nada en esos
 * caminos).
 */
export class DlqHandler {
  private readonly logger = new Logger(DlqHandler.name);
  private producer: Producer | null = null;

  constructor(
    private readonly brokers: string[],
    private readonly clientId = 'dlq-producer',
  ) {}

  /**
   * Ejecuta el handler con reintentos ante errores técnicos; si agota los
   * reintentos, publica el mensaje original en la DLQ y devuelve undefined.
   * @param message Mensaje Kafka original (para republicar en la DLQ).
   * @param fn Handler a ejecutar.
   * @returns Resultado del handler, o undefined si el mensaje fue a la DLQ.
   */
  async run<T>(message: RawKafkaMessage, fn: () => Promise<T>): Promise<T | undefined> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= DLQ_MAX_RETRIES; attempt++) {
      try {
        return await fn();
      } catch (error: unknown) {
        lastError = error as Error;
        if (attempt < DLQ_MAX_RETRIES) {
          const delay = DLQ_RETRY_DELAYS_MS[attempt];
          this.logger.warn(
            `Technical error in ${message.topic} (attempt ${attempt + 1}/${DLQ_MAX_RETRIES + 1}): ${lastError.message}. Retrying in ${delay} ms`,
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    await this.sendToDlq(message, lastError!);
    return undefined;
  }

  /**
   * Publica el mensaje original en `<topic>.dlq` con headers de diagnóstico
   * (error, retryCount, originalTopic, failedAt).
   * @param message Mensaje original.
   * @param error Último error producido tras los reintentos.
   */
  private async sendToDlq(message: RawKafkaMessage, error: Error): Promise<void> {
    const producer = await this.getProducer();
    const dlqTopic = `${message.topic}${DLQ_SUFFIX}`;

    await producer.send({
      topic: dlqTopic,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            error: error.message,
            retryCount: String(DLQ_MAX_RETRIES),
            originalTopic: message.topic,
            failedAt: new Date().toISOString(),
          },
        },
      ],
    });

    this.logger.error(
      `Message sent to ${dlqTopic} after ${DLQ_MAX_RETRIES} retries: ${error.message}`,
    );
  }

  /**
   * Devuelve el productor Kafka, creándolo y conectándolo de forma perezosa.
   * @returns Productor conectado.
   */
  private async getProducer(): Promise<Producer> {
    if (!this.producer) {
      this.producer = new Kafka({
        clientId: this.clientId,
        brokers: this.brokers,
      }).producer({ allowAutoTopicCreation: false });
      await this.producer.connect();
    }
    return this.producer;
  }
}

/**
 * Convierte el mensaje que viene por KafkaContext (donde Nest ya parseó lo
 * parseable del value) en el mensaje original para la DLQ. Si el value quedó
 * como string es porque el JSON era inválido: va verbatim. Si era un objeto,
 * se serializa de vuelta.
 */
export function toRawKafkaMessage(message: {
  topic?: string;
  key?: unknown;
  value?: unknown;
  headers?: Record<string, unknown>;
}): RawKafkaMessage {
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(message.headers ?? {})) {
    headers[k] = Buffer.isBuffer(v) ? v.toString() : String(v);
  }

  return {
    topic: String(message.topic),
    key: Buffer.isBuffer(message.key)
      ? message.key.toString()
      : message.key === null || message.key === undefined
        ? null
        : String(message.key),
    value:
      typeof message.value === 'string'
        ? message.value
        : Buffer.isBuffer(message.value)
          ? message.value.toString()
          : JSON.stringify(message.value),
    headers,
  };
}
