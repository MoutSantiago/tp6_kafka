import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, Types } from 'mongoose';

/** Tipo documento Mongoose de Activation. */
export type ActivationDocument = Activation & Document;

/** Estados posibles de la saga de activación. */
export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

/** Estado de un paso de la saga (billing o provisioning). */
export interface StepStatus {
  status: 'OK' | 'FAILED' | 'PENDING';
  at?: string;
  reason?: string;
}

/** Entrada del historial (timeline) de la activación. */
export interface HistoryEntry {
  eventType: string;
  at: string;
  details?: Record<string, unknown>;
}

/**
 * Esquema Mongo de una activación de servicio: guarda el estado agregado de la
 * saga (PENDING/IN_PROGRESS/ACTIVE/FAILED), el estado de cada paso y el
 * historial completo de eventos.
 */
@Schema({ collection: 'activations', timestamps: true, _id: false })
export class Activation {
  @Prop({ type: String, required: true })
  _id!: string;

  @Prop({ type: String, required: true, index: true })
  customerId!: string;

  @Prop({ type: String, required: true })
  planId!: string;

  @Prop({ type: String, enum: ['PENDING', 'IN_PROGRESS', 'ACTIVE', 'FAILED'], default: 'PENDING' })
  status!: ActivationStatus;

  @Prop({ type: String, enum: ['none', 'billing', 'provisioning'], default: 'none' })
  simulateFailure!: 'none' | 'billing' | 'provisioning';

  @Prop({
    type: {
      billing: { status: { type: String, enum: ['OK', 'FAILED', 'PENDING'] }, at: String, reason: String },
      provisioning: { status: { type: String, enum: ['OK', 'FAILED', 'PENDING'] }, at: String, reason: String },
    },
    default: () => ({
      billing: { status: 'PENDING' },
      provisioning: { status: 'PENDING' },
    }),
  })
  steps!: {
    billing: StepStatus;
    provisioning: StepStatus;
  };

  @Prop({ type: [{ eventType: String, at: String, details: Object }], default: [] })
  history!: HistoryEntry[];

  @Prop({ type: Date, default: Date.now })
  createdAt!: Date;

  @Prop({ type: Date, default: Date.now })
  updatedAt!: Date;
}

/** Esquema Mongoose generado a partir de la clase Activation, con índices por customerId y status. */
export const ActivationSchema = SchemaFactory.createForClass(Activation);

ActivationSchema.index({ customerId: 1 });
ActivationSchema.index({ status: 1 });