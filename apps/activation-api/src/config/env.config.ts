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

/** Configuración general de la aplicación (puerto y entorno). */
export interface AppConfig {
  port: number;
  nodeEnv: string;
}

/** Provee la configuración de Kafka leyendo variables de entorno (KAFKA_BROKERS, KAFKA_CLIENT_ID, KAFKA_GROUP_ID). */
export const kafkaConfig = registerAs('kafka', (): KafkaConfig => ({
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(','),
  clientId: process.env.KAFKA_CLIENT_ID ?? 'activation-api',
  groupId: process.env.KAFKA_GROUP_ID ?? 'activation-api',
}));

/** Provee la configuración de MongoDB leyendo MONGO_URI y MONGO_DATABASE. */
export const mongoConfig = registerAs('mongo', (): MongoConfig => ({
  uri: process.env.MONGO_URI ?? 'mongodb://localhost:27017',
  database: process.env.MONGO_DATABASE ?? 'activation_db',
}));

/** Provee la configuración de la app (PORT, NODE_ENV). */
export const appConfig = registerAs('app', (): AppConfig => ({
  port: parseInt(process.env.PORT ?? '3000', 10),
  nodeEnv: process.env.NODE_ENV ?? 'development',
}));