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

export interface SmtpConfig {
  host: string;
  port: number;
  from: string;
}

export interface AppConfig {
  nodeEnv: string;
}

export const kafkaConfig = registerAs('kafka', (): KafkaConfig => ({
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(','),
  clientId: process.env.KAFKA_CLIENT_ID ?? 'notification-service',
  groupId: process.env.KAFKA_GROUP_ID ?? 'notification-svc',
}));

export const mongoConfig = registerAs('mongo', (): MongoConfig => ({
  uri: process.env.MONGO_URI ?? 'mongodb://localhost:27017',
  database: process.env.MONGO_DATABASE ?? 'notification_db',
}));

export const smtpConfig = registerAs('smtp', (): SmtpConfig => ({
  host: process.env.SMTP_HOST ?? 'mailhog',
  port: parseInt(process.env.SMTP_PORT ?? '1025', 10),
  from: process.env.SMTP_FROM ?? 'activaciones@poc.local',
}));

export const appConfig = registerAs('app', (): AppConfig => ({
  nodeEnv: process.env.NODE_ENV ?? 'development',
}));
