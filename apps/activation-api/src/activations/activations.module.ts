import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { Activation, ActivationSchema } from './activation.schema';
import { ActivationEventsController } from './activation-events.controller';
import { ActivationsController } from './activations.controller';
import { ActivationsService } from './activations.service';

@Module({
  imports: [
    MongooseModule.forFeature([{ name: Activation.name, schema: ActivationSchema }]),
  ],
  controllers: [ActivationsController, ActivationEventsController],
  providers: [ActivationsService],
  exports: [ActivationsService],
})
export class ActivationsModule {}