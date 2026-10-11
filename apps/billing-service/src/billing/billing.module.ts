import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { BillingAccount, BillingAccountSchema } from '../billing/billing-account.schema';
import { ProcessedEvent, ProcessedEventSchema } from '../schemas/processed-event.schema';
import { BillingConsumerService } from '../billing/billing-consumer.service';
import { KafkaProducerService } from '../kafka/kafka-producer.service';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: BillingAccount.name, schema: BillingAccountSchema },
      { name: 'ProcessedEvent', schema: ProcessedEventSchema },
    ]),
  ],
  providers: [BillingConsumerService, KafkaProducerService],
  exports: [KafkaProducerService],
})
export class BillingModule {}