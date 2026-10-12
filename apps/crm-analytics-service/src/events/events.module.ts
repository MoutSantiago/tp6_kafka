import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { EventLog, EventLogSchema } from './event-log.schema';
import { ProcessedEvent, ProcessedEventSchema } from '@activation-poc/kafka-toolkit';
import { EventsService } from './events.service';
import { EventsConsumer } from './events.consumer';
import { EventsController } from './events.controller';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: EventLog.name, schema: EventLogSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [EventsConsumer, EventsController],
  providers: [EventsService],
})
/**
 * Módulo de events: registra los modelos Mongo (EventLog y ProcessedEvent), el
 * consumidor Kafka, el controlador REST y el servicio de registro.
 */
export class EventsModule {}
