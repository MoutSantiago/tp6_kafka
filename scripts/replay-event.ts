/**
 * scripts/replay-event.ts — Republica un evento existente (mismo eventId) en su
 * topic original, leyendo el envelope persistido por crm-analytics.
 *
 * Sirve para probar idempotencia (RNF-03) y re-procesamiento (RF-10):
 * el consumidor debe reconocer el eventId como ya procesado y descartarlo sin
 * duplicar documentos, sin republicar eventos y sin reenviar emails.
 *
 * Uso:
 *   pnpm tsx scripts/replay-event.ts <eventId>
 *
 * Variables de entorno: KAFKA_BROKERS (default localhost:9094),
 * MONGO_URI (default mongodb://localhost:27017).
 */
import { Kafka } from 'kafkajs';
import { MongoClient } from 'mongodb';

async function main() {
  const eventId = process.argv[2];
  if (!eventId) {
    console.error('Uso: pnpm tsx scripts/replay-event.ts <eventId>');
    process.exit(1);
  }

  const mongoUri = process.env.MONGO_URI ?? 'mongodb://localhost:27017';
  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(',');

  // Fuente: el event_log de crm-analytics, que persiste el envelope completo.
  const mongo = new MongoClient(mongoUri, { directConnection: true });
  await mongo.connect();
  try {
    const doc = await mongo
      .db('crm_db')
      .collection('event_log')
      .findOne({ eventId });

    if (!doc) {
      console.error(`Evento ${eventId} no encontrado en crm_db.event_log`);
      process.exit(1);
    }

    const envelope = {
      eventId: doc.eventId,
      eventType: doc.eventType,
      version: 1,
      occurredAt: doc.occurredAt,
      correlationId: doc.correlationId,
      customerId: doc.customerId,
      source: doc.source,
      payload: doc.payload,
    };

    const kafka = new Kafka({ clientId: 'replay-script', brokers });
    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
      topic: doc.topic,
      messages: [
        {
          key: doc.customerId,
          value: JSON.stringify(envelope),
          headers: {
            eventId: doc.eventId,
            correlationId: doc.correlationId,
            eventType: doc.eventType,
            replayed: 'true',
          },
        },
      ],
    });
    await producer.disconnect();

    console.log(
      `Republished ${doc.eventType} (${eventId}) to topic ${doc.topic} with correlationId ${doc.correlationId}`,
    );
  } finally {
    await mongo.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
