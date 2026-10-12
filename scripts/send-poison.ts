/**
 * scripts/send-poison.ts — Publica un mensaje inválido en un topic para probar
 * los reintentos y el DLQ (RNF-04).
 *
 * Uso:
 *   pnpm tsx scripts/send-poison.ts <topic> [broken|missing]
 *
 *   broken   → JSON con sintaxis rota (default)
 *   missing  → JSON válido pero sin los campos del envelope de evento
 *
 * El consumidor debería reintentar 3 veces (1 s, 2 s, 4 s) y publicar el
 * mensaje original en `<topic>.dlq` con `error` y `retryCount` en headers,
 * sin bloquear la partición.
 *
 * Variables de entorno: KAFKA_BROKERS (default localhost:9094).
 */
import { Kafka } from 'kafkajs';

async function main() {
  const topic = process.argv[2];
  const mode = process.argv[3] ?? 'broken';
  if (!topic || !['broken', 'missing'].includes(mode)) {
    console.error('Uso: pnpm tsx scripts/send-poison.ts <topic> [broken|missing]');
    process.exit(1);
  }

  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(',');

  const value =
    mode === 'broken'
      ? '{"eventId": "poison-broken", "eventType":, }' // JSON sintácticamente roto
      : JSON.stringify({ hello: 'world' }); // JSON válido, envelope incompleto

  const kafka = new Kafka({ clientId: 'poison-script', brokers });
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic,
    messages: [
      {
        key: 'poison-key',
        value,
        headers: {
          eventId: `poison-${Date.now()}`,
          eventType: 'PoisonMessage',
        },
      },
    ],
  });
  await producer.disconnect();

  console.log(`Sent "${mode}" poison message to topic ${topic}: ${value}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
