import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Model } from 'mongoose';
import { Kafka, Consumer, Producer, EachMessagePayload, KafkaConfig, IHeaders } from 'kafkajs';
import { EventEnvelope } from '@activation-poc/contracts';

export interface ProcessedEventDocument {
  _id: string;
  eventId: string;
  processedAt: Date;
}

export interface KafkaConsumerOptions {
  brokers: string[];
  groupId: string;
  topics: string[];
  dlqTopicSuffix?: string;
  maxRetries?: number;
}

@Injectable()
export abstract class KafkaConsumerService<T extends EventEnvelope = EventEnvelope>
  implements OnModuleInit, OnModuleDestroy
{
  protected readonly logger = new Logger(this.constructor.name);
  protected kafka!: Kafka;
  protected consumer!: Consumer;
  protected producer!: Producer;
  protected options: KafkaConsumerOptions;
  protected abstract model: Model<ProcessedEventDocument>;

  abstract handleEvent(event: T): Promise<void>;

  constructor(options: KafkaConsumerOptions) {
    this.options = options;
  }

  async onModuleInit(): Promise<void> {
    await this.setupKafka();
    await this.startConsumer();
  }

  async onModuleDestroy(): Promise<void> {
    await this.consumer?.disconnect();
    await this.producer?.disconnect();
  }

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