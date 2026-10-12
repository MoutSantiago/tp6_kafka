import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { Activation, ActivationSchema } from './activation.schema';
import { ProcessedEvent, ProcessedEventSchema } from '@activation-poc/kafka-toolkit';
import { ActivationEventsController } from './activation-events.controller';
import { ActivationsGateway } from './activations.gateway';
import { ActivationsController } from './activations.controller';
import { ActivationsService } from './activations.service';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: Activation.name, schema: ActivationSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [ActivationsController, ActivationEventsController],
  providers: [ActivationsService, ActivationsGateway],
  exports: [ActivationsService],
})
/**
 * Módulo de activaciones: registra los modelos Mongo (Activation y ProcessedEvent),
 * el controlador REST, el consumidor de eventos Kafka, el gateway WebSocket y el
 * servicio de negocio ActivationsService.
 */
export class ActivationsModule {}