import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ProvisioningOrder, ProvisioningOrderSchema } from './provisioning-order.schema';
import { ProvisioningService } from './provisioning.service';
import { ProvisioningController } from './provisioning.controller';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: ProvisioningOrder.name, schema: ProvisioningOrderSchema },
    ]),
  ],
  controllers: [ProvisioningController],
  providers: [ProvisioningService],
})
export class ProvisioningModule {}
