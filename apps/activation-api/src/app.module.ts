import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { appConfig, mongoConfig, kafkaConfig } from './config/env.config';
import { KafkaModule } from './kafka/kafka.module';
import { ActivationsModule } from './activations/activations.module';

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
    ActivationsModule,
  ],
})
/**
 * Módulo raíz de activation-api.
 * Configura conexión a MongoDB, configuración global y registra los módulos
 * de Kafka (productor) y Activations (REST, WebSocket y consumidor de eventos).
 */
export class AppModule {}