import { registerAs } from '@nestjs/config';

export interface KafkaConfig {
  brokers: string[];
  clientId: string;
  groupId: string;
}

export interface AppConfig {
  nodeEnv: string;
}

export const kafkaConfig = registerAs('kafka', (): KafkaConfig => ({
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(','),
  clientId: process.env.KAFKA_CLIENT_ID ?? 'loyalty-service',
  groupId: process.env.KAFKA_GROUP_ID ?? 'loyalty-svc',
}));

export const appConfig = registerAs('app', (): AppConfig => ({
  nodeEnv: process.env.NODE_ENV ?? 'development',
}));
