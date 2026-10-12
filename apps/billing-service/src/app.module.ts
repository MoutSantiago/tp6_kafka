import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { appConfig, mongoConfig, kafkaConfig } from './config/env.config';
import { KafkaModule } from './kafka/kafka.module';
import { BillingModule } from './billing/billing.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [appConfig, mongoConfig, kafkaConfig],
    }),
    MongooseModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (config: { uri: string; database: string }) => ({
        uri: config.uri + (config.uri.includes('?') ? '&' : '?') + 'directConnection=true',
        dbName: config.database,
        retryAttempts: 5,
        retryDelay: 3000,
      }),
      inject: [mongoConfig.KEY],
    }),
    KafkaModule,
    BillingModule,
  ],
})
/**
 * Módulo raíz de billing-service: config global, conexión a MongoDB y registros
 * de los módulos Kafka y Billing.
 */
export class AppModule {}