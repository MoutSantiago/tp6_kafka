import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { EventLog, EventLogDocument } from './event-log.schema';
import type { AnyEvent } from '@activation-poc/contracts';

@Injectable()
export class EventsService {
  private readonly logger = new Logger(EventsService.name);

  constructor(
    @InjectModel(EventLog.name)
    private readonly eventLogModel: Model<EventLogDocument>,
  ) {}

  async recordEvent(event: AnyEvent, topic: string): Promise<void> {
    this.logger.log(
      `Recording ${event.eventType} (${event.eventId}) from ${topic} for correlationId: ${event.correlationId}`,
    );

    // Upsert por eventId: permite reprocesar el historial desde el inicio
    // (RF-10) sin romperse con entregas repetidas.
    await this.eventLogModel
      .updateOne(
        { eventId: event.eventId },
        {
          $setOnInsert: {
            eventType: event.eventType,
            correlationId: event.correlationId,
            customerId: event.customerId,
            topic,
            source: event.source,
            payload: event.payload as unknown as Record<string, unknown>,
            occurredAt: event.occurredAt,
          },
        },
        { upsert: true },
      )
      .exec();
  }

  async findByCorrelationId(correlationId: string): Promise<EventLogDocument[]> {
    return this.eventLogModel
      .find({ correlationId })
      .sort({ occurredAt: 1 })
      .lean()
      .exec() as unknown as EventLogDocument[];
  }
}
