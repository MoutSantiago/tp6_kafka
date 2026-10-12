import { registerAs } from '@nestjs/config';

/** Configuración de conexión a Kafka (brokers, clientId y groupId). */
export interface KafkaConfig {
  brokers: string[];
  clientId: string;
  groupId: string;
}

/** Configuración de conexión a MongoDB (URI y base de datos). */
export interface MongoConfig {
  uri: string;
  database: string;
}

/** Configuración general de la aplicación. */
export interface AppConfig {
  nodeEnv: string;
}

/** Provee la configuración de Kafka leyendo variables de entorno. */
export const kafkaConfig = registerAs('kafka', (): KafkaConfig => ({
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(','),
  clientId: process.env.KAFKA_CLIENT_ID ?? 'billing-service',
  groupId: process.env.KAFKA_GROUP_ID ?? 'billing-svc',
}));

/** Provee la configuración de MongoDB leyendo MONGO_URI y MONGO_DATABASE. */
export const mongoConfig = registerAs('mongo', (): MongoConfig => ({
  uri: process.env.MONGO_URI ?? 'mongodb://localhost:27017',
  database: process.env.MONGO_DATABASE ?? 'billing_db',
}));

/** Provee la configuración general de la app (NODE_ENV). */
export const appConfig = registerAs('app', (): AppConfig => ({
  nodeEnv: process.env.NODE_ENV ?? 'development',
}));