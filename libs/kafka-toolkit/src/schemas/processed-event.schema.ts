import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type ProcessedEventDocument = ProcessedEvent & Document;

@Schema({ collection: 'processed_events' })
export class ProcessedEvent {
  @Prop({ type: String, required: true })
  _id!: string;

  @Prop({ type: String, required: true, unique: true })
  eventId!: string;

  @Prop({ type: Date, default: Date.now, expires: '7d' })
  processedAt!: Date;
}

export const ProcessedEventSchema = SchemaFactory.createForClass(ProcessedEvent);