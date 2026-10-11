import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { ProvisioningOrder, ProvisioningOrderDocument } from './provisioning-order.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import type { ActivationRequestedEvent } from '@activation-poc/contracts';

@Injectable()
export class ProvisioningService {
  private readonly logger = new Logger(ProvisioningService.name);

  constructor(
    @InjectModel(ProvisioningOrder.name)
    private readonly provisioningOrderModel: Model<ProvisioningOrderDocument>,
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

    await this.provisioningOrderModel.create({
      activationId: correlationId,
      customerId,
      planId,
      orderId,
      lineId,
      status: 'COMPLETED',
    });
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
  }
}
