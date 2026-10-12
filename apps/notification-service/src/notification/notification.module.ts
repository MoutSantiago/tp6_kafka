import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { Notification, NotificationSchema } from './notification.schema';
import { ProcessedEvent, ProcessedEventSchema } from '@activation-poc/kafka-toolkit';
import { NotificationService } from './notification.service';
import { NotificationController } from './notification.controller';
import { smtpConfig } from '../config/env.config';

@Module({
  imports: [
    ConfigModule.forFeature(smtpConfig),
    MongooseModule.forFeature([
      { name: Notification.name, schema: NotificationSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [NotificationController],
  providers: [NotificationService],
})
export class NotificationModule {}
