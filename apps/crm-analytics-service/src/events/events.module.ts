import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { EventLog, EventLogSchema } from './event-log.schema';
import { EventsService } from './events.service';
import { EventsConsumer } from './events.consumer';
import { EventsController } from './events.controller';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: EventLog.name, schema: EventLogSchema },
    ]),
  ],
  controllers: [EventsConsumer, EventsController],
  providers: [EventsService],
})
export class EventsModule {}
