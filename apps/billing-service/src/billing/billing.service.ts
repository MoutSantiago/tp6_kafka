import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { BillingAccount, BillingAccountDocument } from './billing-account.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import type { ActivationRequestedEvent } from '@activation-poc/contracts';

@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  constructor(
    @InjectModel(BillingAccount.name)
    private readonly billingAccountModel: Model<BillingAccountDocument>,
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  async handleActivationRequested(event: ActivationRequestedEvent): Promise<void> {
    const { correlationId, customerId } = event;
    const { planId } = event.payload;

    this.logger.log(
      `Processing ActivationRequested for correlationId: ${correlationId}, customerId: ${customerId}`,
    );

    const accountId = `acc-${uuidv4().slice(0, 8)}`;

    await this.billingAccountModel.create({
      activationId: correlationId,
      customerId,
      planId,
      accountId,
      status: 'CREATED',
    });
    this.logger.log(
      `Created billing account ${accountId} for activation ${correlationId}`,
    );

    await this.kafkaProducer.publishBillingAccountCreated(
      correlationId,
      customerId,
      accountId,
      planId,
    );
    this.logger.log(
      `Published BillingAccountCreated for correlationId: ${correlationId}`,
    );
  }
}
