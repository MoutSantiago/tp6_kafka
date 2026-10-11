import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { KafkaConsumerService, KafkaConsumerOptions, ProcessedEventDocument } from '@activation-poc/kafka-toolkit';
import { BillingAccount, BillingAccountDocument } from '../billing/billing-account.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { TOPICS, EVENT_TYPES } from '@activation-poc/contracts';
import type { EventEnvelope, ActivationRequestedPayload } from '@activation-poc/contracts';

@Injectable()
export class BillingConsumerService extends KafkaConsumerService<EventEnvelope<ActivationRequestedPayload>> {
  constructor(
    @InjectModel(BillingAccount.name) private readonly billingAccountModel: Model<BillingAccountDocument>,
    @InjectModel('ProcessedEvent') protected readonly processedEventModel: Model<ProcessedEventDocument>,
    private readonly kafkaProducer: KafkaProducerService,
  ) {
    const options: KafkaConsumerOptions = {
      brokers: process.env.KAFKA_BROKERS?.split(',') || ['localhost:9094'],
      groupId: process.env.KAFKA_GROUP_ID || 'billing-svc',
      topics: [TOPICS.ACTIVATION_REQUESTED],
      maxRetries: 3,
    };
    super(options);
  }

  get model(): Model<ProcessedEventDocument> {
    return this.processedEventModel;
  }

  async handleEvent(event: EventEnvelope<ActivationRequestedPayload>): Promise<void> {
    const payload = event.payload;
    const correlationId = event.correlationId;
    const customerId = event.customerId;
    const eventId = event.eventId;

    this.logger.log(`Processing ActivationRequested for correlationId: ${correlationId}, customerId: ${customerId}, eventId: ${eventId}`);

    const existingAccount = await this.billingAccountModel.findOne({ activationId: correlationId }).exec();
    if (existingAccount) {
      this.logger.log(`Billing account already exists for activationId: ${correlationId}, skipping`);
      return;
    }

    const accountId = `acc-${uuidv4().slice(0, 8)}`;

    const billingAccount = new this.billingAccountModel({
      _id: `ba-${uuidv4().slice(0, 8)}`,
      activationId: correlationId,
      customerId,
      planId: payload.planId,
      accountId,
      status: 'CREATED',
    });

    await billingAccount.save();
    this.logger.log(`Created billing account ${accountId} for activation ${correlationId}`);

    await this.kafkaProducer.publishBillingAccountCreated(
      correlationId,
      customerId,
      accountId,
      payload.planId,
    );
    this.logger.log(`Published BillingAccountCreated for correlationId: ${correlationId}`);
  }
}