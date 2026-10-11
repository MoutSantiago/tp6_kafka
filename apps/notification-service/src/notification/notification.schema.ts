import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type NotificationDocument = Notification & Document;

@Schema({ collection: 'notifications', timestamps: true })
export class Notification {
  @Prop({ type: String, required: true })
  activationId!: string;

  @Prop({ type: String, required: true })
  eventType!: string;

  @Prop({ type: String, required: true })
  customerId!: string;

  @Prop({ type: String, required: true })
  to!: string;

  @Prop({ type: String, required: true })
  subject!: string;

  @Prop({ type: Date, default: Date.now })
  sentAt!: Date;
}

export const NotificationSchema = SchemaFactory.createForClass(Notification);

// Un email por par (activationId, eventType)
NotificationSchema.index({ activationId: 1, eventType: 1 }, { unique: true });
