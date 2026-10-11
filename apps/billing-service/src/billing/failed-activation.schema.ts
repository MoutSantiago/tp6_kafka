import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type FailedActivationDocument = FailedActivation & Document;

/**
 * Marcador de activaciones fallidas vistas por billing.
 * Permite resolver el caso borde en que ActivationFailed llega antes de que
 * billing procese el ActivationRequested: la cuenta se crea y se anula enseguida.
 */
@Schema({ collection: 'failed_activations', timestamps: false })
export class FailedActivation {
  @Prop({ type: String, required: true })
  _id!: string; // activationId

  @Prop({ type: String, required: true })
  reason!: string;

  @Prop({ type: String, required: true })
  at!: string;
}

export const FailedActivationSchema = SchemaFactory.createForClass(FailedActivation);
