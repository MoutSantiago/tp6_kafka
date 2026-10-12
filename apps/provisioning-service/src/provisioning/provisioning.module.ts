import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ProvisioningOrder, ProvisioningOrderSchema } from './provisioning-order.schema';
import { ProcessedEvent, ProcessedEventSchema } from '@activation-poc/kafka-toolkit';
import { ProvisioningService } from './provisioning.service';
import { ProvisioningController } from './provisioning.controller';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: ProvisioningOrder.name, schema: ProvisioningOrderSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [ProvisioningController],
  providers: [ProvisioningService],
})
/**
 * Módulo de provisioning: registra los modelos Mongo (ProvisioningOrder y
 * ProcessedEvent), el consumidor Kafka y el servicio de aprovisionamiento.
 */
export class ProvisioningModule {}
