import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { EventLog, EventLogDocument } from './event-log.schema';
import { consumeIdempotently, ProcessedEvent } from '@activation-poc/kafka-toolkit';
import type { ProcessedEventDocument } from '@activation-poc/kafka-toolkit';
import type { AnyEvent } from '@activation-poc/contracts';

@Injectable()
export class EventsService {
  private readonly logger = new Logger(EventsService.name);

  constructor(
    @InjectModel(EventLog.name)
    private readonly eventLogModel: Model<EventLogDocument>,
    @InjectModel(ProcessedEvent.name)
    private readonly processedEventModel: Model<ProcessedEventDocument>,
  ) {}

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

  async findByCorrelationId(correlationId: string): Promise<EventLogDocument[]> {
    return this.eventLogModel
      .find({ correlationId })
      .sort({ occurredAt: 1 })
      .lean()
      .exec() as unknown as EventLogDocument[];
  }
}
