import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { appConfig, kafkaConfig } from './config/env.config';
import { LoyaltyModule } from './loyalty/loyalty.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [appConfig, kafkaConfig],
    }),
    LoyaltyModule,
  ],
})
/**
 * Módulo raíz de loyalty-service: config global (app y Kafka) y registro del
 * módulo Loyalty. Este servicio no usa MongoDB.
 */
export class AppModule {}
