import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Model } from 'mongoose';
import { Kafka, Consumer, Producer, EachMessagePayload, KafkaConfig, IHeaders } from 'kafkajs';
import { EventEnvelope } from '@activation-poc/contracts';

/** Documento de la colección processed_events usado para idempotencia básica. */
export interface ProcessedEventDocument {
  _id: string;
  eventId: string;
  processedAt: Date;
}

/** Opciones de configuración del consumidor base (brokers, grupo, topics, reintentos y DLQ). */
export interface KafkaConsumerOptions {
  brokers: string[];
  groupId: string;
  topics: string[];
  dlqTopicSuffix?: string;
  maxRetries?: number;
}

@Injectable()
/**
 * Consumidor Kafka base (abstracto): gestiona conexión, suscripción a topics,
 * procesamiento idempotente vía processed_events, commit manual de offsets,
 * reintentos con republicación y envío a DLQ ante errores técnicos.
 * Las subclases implementan handleEvent y proveen el modelo de idempotencia.
 */
export abstract class KafkaConsumerService<T extends EventEnvelope = EventEnvelope>
  implements OnModuleInit, OnModuleDestroy
{
  protected readonly logger = new Logger(this.constructor.name);
  protected kafka!: Kafka;
  protected consumer!: Consumer;
  protected producer!: Producer;
  protected options: KafkaConsumerOptions;
  protected abstract model: Model<ProcessedEventDocument>;

  /**
   * Lógica de negocio del consumidor: procesa un evento ya decodificado.
   * Debe ser implementada por la subclase.
   * @param event Evento a procesar.
   */
  abstract handleEvent(event: T): Promise<void>;

  constructor(options: KafkaConsumerOptions) {
    this.options = options;
  }

  /** Inicializa Kafka (cliente, consumidor y productor) y arranca el consumo. */
  async onModuleInit(): Promise<void> {
    await this.setupKafka();
    await this.startConsumer();
  }

  /** Desconecta consumidor y productor al destruirse el módulo. */
  async onModuleDestroy(): Promise<void> {
    await this.consumer?.disconnect();
    await this.producer?.disconnect();
  }

  /** Crea y configura el cliente Kafka, el consumidor (con reintentos) y el productor. */
  protected async setupKafka(): Promise<void> {
    const kafkaConfig: KafkaConfig = {
      clientId: `${this.options.groupId}-consumer`,
      brokers: this.options.brokers,
      retry: {
        initialRetryTime: 100,
        retries: 5,
        maxRetryTime: 30000,
        factor: 2,
      },
    };

    this.kafka = new Kafka(kafkaConfig);
    this.consumer = this.kafka.consumer({
      groupId: this.options.groupId,
      maxInFlightRequests: 1, // Asegurar orden de procesamiento
    });

    this.producer = this.kafka.producer();
    await this.producer.connect();
  }

  /** Conecta el consumidor, lo suscribe a los topics y arranca el loop con commit manual. */
  protected async startConsumer(): Promise<void> {
    await this.consumer.connect();

    for (const topic of this.options.topics) {
      await this.consumer.subscribe({
        topic,
        fromBeginning: false // Solo nuevos mensajes
      });
    }

    await this.consumer.run({
      autoCommit: false, // Desactivar commit automático
      eachMessage: async (payload: EachMessagePayload) => {
        await this.processMessage(payload);
      },
    });

    this.logger.log(`Consumer started for topics: ${this.options.topics.join(', ')}`);
  }

  /**
   * Procesa un mensaje individual: verifica idempotencia por eventId, decodifica
   * el evento, lo procesa, lo marca como procesado y commitea el offset. Ante
   * errores delega en handleProcessingError (reintento/DLQ).
   * @param payload Mensaje Kafka recibido.
   */
  protected async processMessage(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;
    const eventId = message.headers?.eventId?.toString() || '';

    if (!eventId) {
      this.logger.warn(`Message in topic ${topic} has no eventId, skipping`);
      await this.commitOffset(payload);
      return;
    }

    // Verificar idempotencia
    const alreadyProcessed = await this.isEventProcessed(eventId);
    if (alreadyProcessed) {
      this.logger.log(`Event ${eventId} already processed, skipping`);
      await this.commitOffset(payload);
      return;
    }

    try {
      // Decodificar el mensaje
      const event = JSON.parse(message.value?.toString() || '{}') as T;

      // Procesar el evento
      await this.handleEvent(event);

      // Marcar como procesado
      await this.markEventAsProcessed(eventId);

      // Confirmar el offset
      await this.commitOffset(payload);

      this.logger.debug(`Successfully processed event ${eventId} from topic ${topic}`);
    } catch (error: unknown) {
      const err = error as Error;
      this.logger.error(`Error processing message from topic ${topic}: ${err.message}`, err.stack);

      // Intentar reintento o enviar a DLQ
      await this.handleProcessingError(payload, err);
    }
  }

  /**
   * Commitea manualmente el offset siguiente al mensaje procesado.
   * @param payload Mensaje cuyo offset se quiere confirmar.
   */
  protected async commitOffset(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;
    try {
      await this.consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (parseInt(message.offset) + 1).toString(),
        },
      ]);
    } catch (error: unknown) {
      const err = error as Error;
      this.logger.error(`Failed to commit offset for topic ${topic}, partition ${partition}: ${err.message}`);
    }
  }

  /**
   * Verifica si un eventId ya fue procesado (consulta en processed_events).
   * @param eventId ID del evento a verificar.
   * @returns true si ya fue procesado; false si no o ante error de consulta.
   */
  protected async isEventProcessed(eventId: string): Promise<boolean> {
    try {
      const doc = await this.model.findById(eventId).exec();
      return !!doc;
    } catch (error: unknown) {
      const err = error as Error;
      this.logger.error(`Error checking if event ${eventId} was processed: ${err.message}`);
      // En caso de error, asumimos que no fue procesado para evitar pérdidas
      return false;
    }
  }

  /**
   * Marca un evento como procesado insertando su eventId en processed_events.
   * @param eventId ID del evento.
   * @throws Propaga el error para permitir reintento o envío a DLQ.
   */
  protected async markEventAsProcessed(eventId: string): Promise<void> {
    try {
      const doc = new this.model({
        _id: eventId,
        eventId,
        processedAt: new Date(),
      });
      await doc.save();
    } catch (error: unknown) {
      const err = error as Error;
      this.logger.error(`Error marking event ${eventId} as processed: ${err.message}`);
      throw error; // Propagamos el error para reintentar o enviar a DLQ
    }
  }

  /**
   * Maneja un error técnico de procesamiento: republica el mensaje en el mismo
   * topic con el contador de reintentos incrementado, o lo envía a la DLQ si ya
   * agotó los reintentos, commiteando el offset para no bloquear la partición.
   * @param payload Mensaje que falló.
   * @param error Error arrojado durante el procesamiento.
   */
  protected async handleProcessingError(payload: EachMessagePayload, error: Error): Promise<void> {
    const { topic, partition, message } = payload;
    const eventId = message.headers?.eventId?.toString() || 'unknown';
    const retryCount = parseInt(message.headers?.retryCount?.toString() || '0');
    const maxRetries = this.options.maxRetries || 3;

    if (retryCount < maxRetries) {
      // Reintento: re-publicar el mensaje en el mismo topic con contador incrementado
      this.logger.warn(`Retrying event ${eventId} (attempt ${retryCount + 1}/${maxRetries})`);

      // Convertir IHeaders a un objeto plano
      const headers: Record<string, string> = {};
      if (message.headers) {
        Object.keys(message.headers).forEach(key => {
          const value = message.headers![key];
          headers[key] = value ? value.toString() : '';
        });
      }

      headers['retryCount'] = (retryCount + 1).toString();
      headers['originalTopic'] = topic;
      headers['error'] = error.message;

      await this.producer.send({
        topic,
        messages: [
          {
            key: message.key,
            value: message.value,
            headers,
          },
        ],
      });

      // Confirmar el offset para no bloquear la partición
      await this.commitOffset(payload);
    } else {
      // DLQ: enviar al topic DLQ correspondiente
      const dlqTopic = `${topic}${this.options.dlqTopicSuffix || '.dlq'}`;
      this.logger.error(`Sending event ${eventId} to DLQ ${dlqTopic} after ${maxRetries} retries`);

      try {
        // Convertir IHeaders a un objeto plano
        const headers: Record<string, string> = {};
        if (message.headers) {
          Object.keys(message.headers).forEach(key => {
            const value = message.headers![key];
            headers[key] = value ? value.toString() : '';
          });
        }

        headers['error'] = error.message;
        headers['originalTopic'] = topic;
        headers['failedAt'] = new Date().toISOString();

        await this.producer.send({
          topic: dlqTopic,
          messages: [
            {
              key: message.key,
              value: message.value,
              headers,
            },
          ],
        });

        // Confirmar el offset del mensaje original para no bloquear la partición
        await this.commitOffset(payload);
      } catch (dlqError: unknown) {
        const err = dlqError as Error;
        this.logger.error(`Failed to send message to DLQ: ${err.message}`);

        // Si falla el envío a DLQ, no confirmamos el offset para que se reintente
        throw dlqError;
      }
    }
  }
}