import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type BillingAccountDocument = BillingAccount & Document;

export type BillingAccountStatus = 'CREATED' | 'CANCELLED';

@Schema({ collection: 'billing_accounts', timestamps: true })
export class BillingAccount {
  @Prop({ type: String, required: true, unique: true })
  activationId!: string;

  @Prop({ type: String, required: true })
  customerId!: string;

  @Prop({ type: String, required: true })
  planId!: string;

  @Prop({ type: String, required: true })
  accountId!: string;

  @Prop({ type: String, enum: ['CREATED', 'CANCELLED'], default: 'CREATED' })
  status!: BillingAccountStatus;

  @Prop({ type: String, default: '' })
  cancelReason!: string;
}

export const BillingAccountSchema = SchemaFactory.createForClass(BillingAccount);
