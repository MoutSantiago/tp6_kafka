import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type EventLogDocument = EventLog & Document;

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

export const EventLogSchema = SchemaFactory.createForClass(EventLog);

EventLogSchema.index({ correlationId: 1 });
EventLogSchema.index({ eventId: 1 }, { unique: true });
