import { Module, Global } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { kafkaConfig } from '../config/env.config';
import { KafkaProducerService } from './kafka-producer.service';

@Global()
@Module({
  imports: [
    ConfigModule.forFeature(kafkaConfig),
    ClientsModule.registerAsync([
      {
        name: 'KAFKA_CLIENT',
        imports: [ConfigModule],
        useFactory: (config: { brokers: string[]; clientId: string; groupId: string }) => ({
          transport: Transport.KAFKA,
          options: {
            client: {
              clientId: config.clientId,
              brokers: config.brokers,
            },
            producer: {
              allowAutoCreateTopics: false,
              transactionTimeout: 30000,
            },
            consumer: {
              groupId: config.groupId,
              allowAutoCreateTopics: false,
              sessionTimeout: 30000,
              heartbeatInterval: 10000,
            },
          },
        }),
        inject: [kafkaConfig.KEY],
      },
    ]),
  ],
  providers: [KafkaProducerService],
  exports: [ClientsModule, KafkaProducerService],
})
export class KafkaModule {}
