import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { ProvisioningOrder, ProvisioningOrderDocument } from './provisioning-order.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { consumeIdempotently, ProcessedEvent, ProcessedEventDocument } from '@activation-poc/kafka-toolkit';
import type { ActivationRequestedEvent } from '@activation-poc/contracts';

@Injectable()
export class ProvisioningService {
  private readonly logger = new Logger(ProvisioningService.name);

  constructor(
    @InjectModel(ProvisioningOrder.name)
    private readonly provisioningOrderModel: Model<ProvisioningOrderDocument>,
    @InjectModel(ProcessedEvent.name)
    private readonly processedEventModel: Model<ProcessedEventDocument>,
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  async handleActivationRequested(event: ActivationRequestedEvent): Promise<void> {
    const { correlationId, customerId } = event;
    const { planId } = event.payload;

    this.logger.log(
      `Processing ActivationRequested for correlationId: ${correlationId}, customerId: ${customerId}`,
    );

    // Simular demora de aprovisionamiento: entre 1 y 3 segundos
    const delayMs = 1000 + Math.floor(Math.random() * 2000);
    await new Promise((resolve) => setTimeout(resolve, delayMs));

    const orderId = `ord-${uuidv4().slice(0, 8)}`;
    const lineId = `line-${uuidv4().slice(0, 8)}`;

    // Idempotencia: el insert en processed_events y la creación de la orden van en
    // la misma transacción. Una entrega duplicada del mismo eventId se descarta sin
    // crear una segunda orden (y sin republicar el evento resultado).
    const { result, value } = await consumeIdempotently(
      this.processedEventModel,
      event.eventId,
      async (session) => {
        // Fallo de negocio simulado: no es un error técnico. No se crea la orden y se
        // publica ProvisioningFailed como resultado de negocio en provisioning.events.
        if (event.simulateFailure === 'provisioning') {
          return { ok: false as const, reason: 'Fallo simulado en provisioning' };
        }
        await this.provisioningOrderModel.create(
          [
            {
              activationId: correlationId,
              customerId,
              planId,
              orderId,
              lineId,
              status: 'COMPLETED',
            },
          ],
          { session },
        );
        return { ok: true as const };
      },
    );

    if (result === 'duplicate') {
      this.logger.warn(
        `Duplicate ActivationRequested ${event.eventId} for ${correlationId}, skipping`,
      );
      return;
    }

    const outcome = value!;
    if (outcome.ok) {
      this.logger.log(
        `Created provisioning order ${orderId} for activation ${correlationId}`,
      );
      await this.kafkaProducer.publishProvisioningCompleted(
        correlationId,
        customerId,
        orderId,
        lineId,
      );
      this.logger.log(
        `Published ProvisioningCompleted for correlationId: ${correlationId}`,
      );
    } else {
      this.logger.warn(
        `Simulated provisioning failure for activation ${correlationId}`,
      );
      await this.kafkaProducer.publishProvisioningFailed(
        correlationId,
        customerId,
        orderId,
        outcome.reason,
      );
      this.logger.log(
        `Published ProvisioningFailed for correlationId: ${correlationId}`,
      );
    }
  }
}
