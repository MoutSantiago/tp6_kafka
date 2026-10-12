import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

/** Tipo documento Mongoose de BillingAccount. */
export type BillingAccountDocument = BillingAccount & Document;

/** Estados posibles de una cuenta de facturación. */
export type BillingAccountStatus = 'CREATED' | 'CANCELLED';

/**
 * Esquema Mongo de una cuenta de facturación creada durante la saga de activación
 * (una por activationId).
 */
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

/** Esquema Mongoose generado a partir de la clase BillingAccount. */
export const BillingAccountSchema = SchemaFactory.createForClass(BillingAccount);
