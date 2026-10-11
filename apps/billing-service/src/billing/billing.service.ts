import { Injectable, OnModuleInit, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { v4 as uuidv4 } from 'uuid';
import { EventPattern, Payload, Ctx, KafkaContext } from '@nestjs/microservices';
import { BillingAccount, BillingAccountDocument } from './billing-account.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { EVENT_TYPES, TOPICS } from '@activation-poc/contracts';
import type { EventEnvelope, ActivationRequestedPayload } from '@activation-poc/contracts';

type ActivationRequestedEvent = EventEnvelope<ActivationRequestedPayload>;

@Injectable()
export class BillingService implements OnModuleInit {
  private readonly logger = new Logger(BillingService.name);

  constructor(
    @InjectModel(BillingAccount.name) private readonly billingAccountModel: Model<BillingAccountDocument>,
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  async onModuleInit(): Promise<void> {
    this.logger.log('BillingService initialized');
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(
    @Payload() event: ActivationRequestedEvent,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
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