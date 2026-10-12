import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

/** Tipo documento Mongoose de ProcessedEvent. */
export type ProcessedEventDocument = ProcessedEvent & Document;

/**
 * Esquema Mongo de la colección processed_events: marcador de eventos ya
 * consumidos para garantizar procesamiento exactamente-una-vez (eventId único,
 * con TTL de 7 días en processedAt).
 */
@Schema({ collection: 'processed_events' })
export class ProcessedEvent {
  @Prop({ type: String, required: true })
  _id!: string;

  @Prop({ type: String, required: true, unique: true })
  eventId!: string;

  @Prop({ type: Date, default: Date.now, expires: '7d' })
  processedAt!: Date;
}

/** Esquema Mongoose generado a partir de la clase ProcessedEvent. */
export const ProcessedEventSchema = SchemaFactory.createForClass(ProcessedEvent);