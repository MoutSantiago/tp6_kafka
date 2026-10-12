import { NestFactory } from "@nestjs/core";
import { ValidationPipe } from "@nestjs/common";
import { SwaggerModule, DocumentBuilder } from "@nestjs/swagger";
import { MicroserviceOptions, Transport } from "@nestjs/microservices";
import { AppModule } from "./app.module";
import { appConfig, kafkaConfig } from "./config/env.config";
import { TOPICS } from "@activation-poc/contracts";

/**
 * Punto de entrada de activation-api.
 * Levanta la app HTTP con Swagger y validación global, conecta el microservicio
 * Kafka (suscrito a billing.events y provisioning.events) y expone la API REST.
 */
async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  const configService = app.get(appConfig.KEY);
  const port = configService.port;

  const kafkaOptions: MicroserviceOptions = {
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: kafkaConfig().clientId,
        brokers: kafkaConfig().brokers,
      },
      consumer: {
        groupId: kafkaConfig().groupId,
        allowAutoCreateTopics: false,
        sessionTimeout: 30000,
        heartbeatInterval: 10000,
      },
      subscribe: {
        fromBeginning: false,
        topics: [TOPICS.BILLING_EVENTS, TOPICS.PROVISIONING_EVENTS],
      },
    },
  };

  app.connectMicroservice<MicroserviceOptions>(kafkaOptions);
  await app.startAllMicroservices();

  app.enableCors();
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
    }),
  );

  const config = new DocumentBuilder()
    .setTitle("Activation API")
    .setDescription("API for managing service activations")
    .setVersion("1.0")
    .addTag("activations")
    .build();
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup("docs", app, document);

  await app.listen(port);
  console.log(`Activation API running on http://localhost:${port}`);
  console.log(`Swagger docs available at http://localhost:${port}/docs`);
}

bootstrap();
