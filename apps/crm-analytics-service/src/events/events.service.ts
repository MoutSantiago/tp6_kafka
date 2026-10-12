import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { EventLog, EventLogDocument } from './event-log.schema';
import { consumeIdempotently, ProcessedEvent } from '@activation-poc/kafka-toolkit';
import type { ProcessedEventDocument } from '@activation-poc/kafka-toolkit';
import type { AnyEvent } from '@activation-poc/contracts';

/**
 * Servicio de negocio de crm-analytics-service: registra todos los eventos de
 * la POC en un event log de Mongo de forma idempotente y expone consultas.
 */
@Injectable()
export class EventsService {
  private readonly logger = new Logger(EventsService.name);

  constructor(
    @InjectModel(EventLog.name)
    private readonly eventLogModel: Model<EventLogDocument>,
    @InjectModel(ProcessedEvent.name)
    private readonly processedEventModel: Model<ProcessedEventDocument>,
  ) {}

  /**
   * Registra un evento en el event_log dentro de una transacción idempotente;
   * las entregas duplicadas se descartan sin duplicar documentos.
   * @param event Evento de negocio recibido.
   * @param topic Topic de origen del evento.
   */
  async recordEvent(event: AnyEvent, topic: string): Promise<void> {
    this.logger.log(
      `Recording ${event.eventType} (${event.eventId}) from ${topic} for correlationId: ${event.correlationId}`,
    );

    // Idempotencia: insert de eventId + efecto (el registro en event_log) en la
    // misma transacción. Si se reprocesa el historial desde el inicio, las
    // entregas repetidas se descartan (insertE11000) sin duplicar documentos.
    const { result } = await consumeIdempotently(
      this.processedEventModel,
      event.eventId,
      async (session) => {
        await this.eventLogModel.create(
          [
            {
              eventId: event.eventId,
              eventType: event.eventType,
              correlationId: event.correlationId,
              customerId: event.customerId,
              topic,
              source: event.source,
              payload: event.payload as unknown as Record<string, unknown>,
              occurredAt: event.occurredAt,
            },
          ],
          { session },
        );
      },
    );

    if (result === 'duplicate') {
      this.logger.warn(`Duplicate event ${event.eventId}, skipping`);
    }
  }

  /**
   * Busca en el log todos los eventos de un correlationId, ordenados por fecha.
   * @param correlationId ID de correlación a consultar.
   * @returns Eventos registrados, en orden de ocurrencia.
   */
  async findByCorrelationId(correlationId: string): Promise<EventLogDocument[]> {
    return this.eventLogModel
      .find({ correlationId })
      .sort({ occurredAt: 1 })
      .lean()
      .exec() as unknown as EventLogDocument[];
  }
}
