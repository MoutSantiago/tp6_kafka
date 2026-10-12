import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

/** Tipo documento Mongoose de EventLog. */
export type EventLogDocument = EventLog & Document;

/**
 * Esquema Mongo del registro de eventos (event log) de la POC: un documento por
 * evento de negocio recibido, indexado por correlationId y con eventId único.
 */
@Schema({ collection: 'event_log', timestamps: true })
export class EventLog {
  @Prop({ type: String, required: true, unique: true })
  eventId!: string;

  @Prop({ type: String, required: true })
  eventType!: string;

  @Prop({ type: String, required: true })
  correlationId!: string;

  @Prop({ type: String, required: true })
  customerId!: string;

  @Prop({ type: String, required: true })
  topic!: string;

  @Prop({ type: String, required: true })
  source!: string;

  @Prop({ type: Object, default: {} })
  payload!: Record<string, unknown>;

  @Prop({ type: String, required: true })
  occurredAt!: string;
}

/** Esquema Mongoose generado a partir de la clase EventLog. */
export const EventLogSchema = SchemaFactory.createForClass(EventLog);

EventLogSchema.index({ correlationId: 1 });
EventLogSchema.index({ eventId: 1 }, { unique: true });
