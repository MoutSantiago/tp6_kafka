import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { kafkaConfig } from './config/env.config';
import { TOPICS } from '@activation-poc/contracts';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  const configService = app.get(kafkaConfig.KEY);
  const kafkaOptions: MicroserviceOptions = {
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: configService.clientId,
        brokers: configService.brokers,
      },
      consumer: {
        groupId: configService.groupId,
        allowAutoCreateTopics: false,
        sessionTimeout: 30000,
        heartbeatInterval: 10000,
      },
      subscribe: {
        fromBeginning: false,
        topics: [TOPICS.ACTIVATION_REQUESTED],
      },
    },
  };

  app.connectMicroservice<MicroserviceOptions>(kafkaOptions);

  await app.startAllMicroservices();
  await app.init();

  console.log('Provisioning Service started and listening for ActivationRequested events');
}

bootstrap();
