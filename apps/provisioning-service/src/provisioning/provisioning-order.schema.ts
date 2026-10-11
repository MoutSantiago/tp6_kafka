import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type ProvisioningOrderDocument = ProvisioningOrder & Document;

export type ProvisioningOrderStatus = 'COMPLETED';

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

export const ProvisioningOrderSchema = SchemaFactory.createForClass(ProvisioningOrder);
