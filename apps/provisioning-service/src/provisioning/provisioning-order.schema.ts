import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

/** Tipo documento Mongoose de ProvisioningOrder. */
export type ProvisioningOrderDocument = ProvisioningOrder & Document;

/** Estados posibles de una orden de aprovisionamiento. */
export type ProvisioningOrderStatus = 'COMPLETED';

/**
 * Esquema Mongo de una orden de aprovisionamiento creada durante la saga de
 * activación (una por activationId).
 */
@Schema({ collection: 'provisioning_orders', timestamps: true })
export class ProvisioningOrder {
  @Prop({ type: String, required: true, unique: true })
  activationId!: string;

  @Prop({ type: String, required: true })
  customerId!: string;

  @Prop({ type: String, required: true })
  planId!: string;

  @Prop({ type: String, required: true })
  orderId!: string;

  @Prop({ type: String, required: true })
  lineId!: string;

  @Prop({ type: String, enum: ['COMPLETED'], default: 'COMPLETED' })
  status!: ProvisioningOrderStatus;
}

/** Esquema Mongoose generado a partir de la clase ProvisioningOrder. */
export const ProvisioningOrderSchema = SchemaFactory.createForClass(ProvisioningOrder);
