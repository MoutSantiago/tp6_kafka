import { registerAs } from '@nestjs/config';

export interface KafkaConfig {
  brokers: string[];
  clientId: string;
  groupId: string;
}

export interface MongoConfig {
  uri: string;
  database: string;
}

export interface AppConfig {
  nodeEnv: string;
  port: number;
}

export const kafkaConfig = registerAs('kafka', (): KafkaConfig => ({
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(','),
  clientId: process.env.KAFKA_CLIENT_ID ?? 'crm-analytics-service',
  groupId: process.env.KAFKA_GROUP_ID ?? 'crm-analytics',
}));

export const mongoConfig = registerAs('mongo', (): MongoConfig => ({
  uri: process.env.MONGO_URI ?? 'mongodb://localhost:27017',
  database: process.env.MONGO_DATABASE ?? 'crm_db',
}));

export const appConfig = registerAs('app', (): AppConfig => ({
  nodeEnv: process.env.NODE_ENV ?? 'development',
  port: parseInt(process.env.PORT ?? '3004', 10),
}));
