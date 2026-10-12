# Arquitectura — POC Activación de Servicios con Kafka

Documento de arquitectura **ajustado a lo realmente implementado**. Cuando difiere de `docs/spec.md`, los desvíos están listados al final con su motivo.

## 1. Resumen

La activación de un plan se resuelve publicando un único evento en Kafka. La UI llama a `activation-api` solo para crear la activación (`POST /activations`), guardar su estado inicial y publicar `ActivationRequested`. A partir de ahí:

- `billing-service` y `provisioning-service` consumen el mismo evento en paralelo y publican su resultado (éxito o fallo de negocio).
- `activation-api` agrega los resultados, actualiza la saga y publica `ActivationCompleted` o `ActivationFailed`.
- `billing-service` consume `ActivationFailed` y compensa (anula la cuenta).
- `notification-service` consume el cierre y manda el email por Mailhog.
- `crm-analytics-service` consume los 4 topics y persiste cada evento en `event_log`.
- `loyalty-service` (profile opcional) consume todo desde el inicio, y solo loguea.

La UI recibe cada cambio de una activación en tiempo real por WebSocket (socket.io).

Todo corre local con `docker compose up -d` sin pasos manuales.

## 2. Stack

| Componente | Tecnología | Observación |
| :--- | :--- | :--- |
| Mensajería | Apache Kafka 3.7.2, modo KRaft, 1 broker | Listener interno `kafka:9092` (red Docker), externo `localhost:9094` (host) |
| Servicios | NestJS 10 + `@nestjs/microservices` (Kafka) | Microservicio conectado con `app.connectMicroservice` + `ClientKafka` para publicar |
| UI | React 18 + Vite + `socket.io-client` | Servida por nginx en el build; `vite dev` en desarrollo |
| Persistencia | MongoDB 7.0 (replica set de 1 nodo) | Transacciones para el helper de idempotencia |
| Email | Mailhog v1.0.1 | SMTP en 1025, bandeja en 8025 |
| Observabilidad | kafka-ui v0.7.1 | Topics, mensajes, offsets, lag en vivo |
| Imágenes base | `node:20.18.1-bookworm-slim`, `nginx:1.25.4-alpine` | Versiones fijadas, sin `latest` |

## 3. Estructura del monorepo (pnpm workspaces)

```
apps/
  activation-api/          # REST + WebSocket + agregador de la saga
  billing-service/         # crea/anula cuentas de facturación simuladas
  provisioning-service/    # crea órdenes simuladas (espera aleatoria 1–3 s)
  notification-service/    # emails por Mailhog
  crm-analytics-service/   # event_log + GET /events?correlationId=
  loyalty-service/         # consumidor de solo-logueo (profile "loyalty")
  demo-ui/                 # React + Vite, timeline vía WebSocket
libs/
  contracts/               # envelope de eventos, tipos, topics, event types
  kafka-toolkit/           # consumeIdempotently() + DlqHandler + assertEventEnvelope
docker/
  create-topics.sh         # crea los 4 topics de negocio + sus DLQ (3 particiones c/u)
docs/
  spec.md, demo-script.md, architecture.md
scripts/
  replay-event.ts          # republica un evento conservando su eventId
  send-poison.ts           # mensaje inválido para probar reintentos/DLQ
```

Los servicios usan el patrón `@EventPattern` en **controllers** (Nest 10 solo registra handlers de mensajes en controllers, no en providers), con un service inyectado para la lógica.

Env vars comunes: `KAFKA_BROKERS`, `KAFKA_CLIENT_ID`, `KAFKA_GROUP_ID`, `MONGO_URI`, `MONGO_DATABASE`, `PORT`, `SMTP_HOST`/`SMTP_PORT`/`SMTP_FROM` (notification), `VITE_API_URL` (demo-ui, en build arg).

## 4. Topics, eventos y consumer groups

Todos los mensajes siguen el envelope de `libs/contracts`:

```ts
{ eventId, eventType, version, occurredAt, correlationId, customerId, source, simulateFailure?, payload }
```

Reglas de publicación implementadas:
- `correlationId` = `activationId` de la activación.
- **key** del mensaje = `customerId` (mismo cliente → misma partición → orden).
- Headers: `eventId`, `correlationId`, `eventType`.

| Topic | Eventos | Produce | Consume (group real de Kafka) |
| :--- | :--- | :--- | :--- |
| `activation.requested` | `ActivationRequested` | activation-api | billing-svc-server, provisioning-svc-server, crm-analytics-server, loyalty-svc-server |
| `billing.events` | `BillingAccountCreated`, `BillingFailed`, `BillingAccountCancelled` | billing-service | activation-api-server, crm-analytics-server, loyalty-svc-server |
| `provisioning.events` | `ProvisioningCompleted`, `ProvisioningFailed` | provisioning-service | activation-api-server, crm-analytics-server, loyalty-svc-server |
| `activation.events` | `ActivationCompleted`, `ActivationFailed` | activation-api | notification-svc-server, billing-svc-server, crm-analytics-server, loyalty-svc-server |

Nota: KafkaJS sufija el groupId configurado con `-server` (por ejemplo, configurar `billing-svc` en `KAFKA_GROUP_ID` produce el grupo real `billing-svc-server`). Cada servicio configura su group fijo vía env.

## 5. La saga y el agregador (activation-api)

Colección `activations` en `activation_db`:

```json
{
  "_id": "act-xxxxxxxx", "customerId": "...", "planId": "...",
  "status": "PENDING|IN_PROGRESS|ACTIVE|FAILED",
  "simulateFailure": "none|billing|provisioning",
  "steps": { "billing": {"status","at","reason"}, "provisioning": {...} },
  "history": [ { "eventType", "at", "details" } ]
}
```

Estados: `PENDING → IN_PROGRESS → ACTIVE | FAILED`.

Algoritmo (todo atómico con `findOneAndUpdate`, diseñado para resultados concurrentes):

1. **Registrar el resultado del step**: `findOneAndUpdate({_id, 'steps.X.status': 'PENDING'}, ...)` con `$set` del step y `$push` al history. No filtra por estado global: un resultado que llega **después** de un estado terminal queda registrado (step + history) pero no cambia nada. `BillingAccountCancelled` se registra igual en el history (con `accountId` y `reason`) sin tocar el estado de la saga.
2. Si es `FAILED`, transición terminal: `findOneAndUpdate({_id, status: {$in: ['PENDING','IN_PROGRESS']}}, ...)` — filtros atómicos: solo un consumidor la gana y publica `ActivationFailed`.
3. Si es `OK`: `updateOne({_id, status:'PENDING'} → IN_PROGRESS)` (inofensiva en carrera); si ambos steps OK, transición: `findOneAndUpdate({_id, status:'IN_PROGRESS', steps.billing OK, steps.provisioning OK} → ACTIVE)` — solo un ganador publica `ActivationCompleted`.

WebSocket: por cada entrada que se agrega al history se emite `{ activationId, eventType, at, status }` a la room del `activationId` y a la room global `all` (socket.io deduplica si un cliente está en ambas). CORS permite `localhost:5173` y páginas locales con `Origin: null`.

## 6. Compensación y caso borde (billing-service)

`billing-service` consume `activation.events`. Ante `ActivationFailed`:

1. Guarda un **marcador** en `failed_activations` (colección de `billing_db`, `_id` = activationId) — *antes* de intentar cancelar.
2. Intenta cancelar atómicamente: `findOneAndUpdate({activationId, status:'CREATED'} → CANCELLED)`. Solo quien gana la actualización publica `BillingAccountCancelled`.

**Caso borde** (el evento `ActivationFailed` llega antes de que billing procese el `ActivationRequested`): la creación chequea el marcador **después** de crear la cuenta; si existe, anula de inmediato. La atomicidad del findOneAndUpdate y el orden marcador-primero garantizan que siempre queda CANCELLED y que `BillingAccountCancelled` se publica exactamente una vez.

## 7. Idempotencia (RNF-03)

`libs/kafka-toolkit` expone `consumeIdempotently(model, eventId, effect)`:

- En **una transacción de Mongo** (por eso el replica set de un nodo) inserta `{_id: eventId, eventId, processedAt}` en `processed_events` (índice único por `eventId`, TTL de 7 días) y ejecuta el efecto del consumidor.
- Si el insert falla con E11000 (ya procesado), la transacción aborta y el evento se descarta sin aplicar el efecto.
- Efectos externos (publicar en Kafka, emitir WS) se ejecutan después del commit.
- En notification el efecto incluye el envío SMTP: si el envío falla técnicamente, la tx aborta y el evento no queda marcado (se reentrega).

Aplicado en los 4 consumidores y en el agregador: las 3 escrituras de la saga van en la misma tx que el marcador. `scripts/replay-event.ts` república un evento conservando su `eventId` para verificarlo.

## 8. Reintentos y DLQ (RNF-04)

`DlqHandler.run(rawMessage, fn)` envuelve cada handler de consumidor:

- Ante error técnico: reintenta hasta **3 veces** con esperas de **1 s, 2 s, 4 s**.
- Si sigue fallando, publica el mensaje original (verbatim, con raw value, key y headers) en **`<topic>.dlq`** con headers `error`, `retryCount`, `originalTopic`, `failedAt`, y retorna normalmente → Nest commitea el offset → la partición no se bloquea.
- `assertEventEnvelope(event)` valida el envelope; un JSON roto o sin campos es considerado error técnico (Nest pasa el JSON roto como string porque su parser traga el `SyntaxError`).
- Los fallos de negocio (`simulateFailure: billing|provisioning`) no pasan por acá: no lanzan excepciones, se publican como `BillingFailed`/`ProvisioningFailed`.

`scripts/send-poison.ts <topic> [broken|missing]` publica mensajes inválidos para probar el flujo.

## 9. Puertos

| Servicio | Puerto | Nota |
| :--- | :--- | :--- |
| kafka | 9092 (red docker), 9094 (host) | solo swarm interno en compose |
| kafka-ui | 8080 | |
| mongo | 27017 | |
| mailhog | 1025 (SMTP), 8025 (UI) | |
| activation-api | 3000 | REST + WebSocket + Swagger docs `/docs` |
| crm-analytics-service | 3004 | `GET /events?correlationId=...` (fuera de lo mostrado en el spec) |
| demo-ui | 5173 | nginx en Docker, vite dev local |
| billing/provisioning/notification/loyalty | - | sin HTTP |

## 10. Desvíos y decisiones con respecto a `docs/spec.md`

1. **Outbox transaccional: NO implementado** (quedó en fase 4). Se publica directo tras guardar y hay una ventana de pérdida si el proceso muere entre el guardado y la publicación.
2. **Fase 4 (Schema Registry, OpenTelemetry) no implementada** — era opcional.
3. **Handlers de eventos en controllers, no en services.** En Nest 10 los `@EventPattern` solo se registran en controllers; el service recibe la llamada delegada.
4. **Group real de KafkaJS**: el valor configurado se sufija con `-server` (y el `ClientKafka` con `-client`). Los nombres visibles en kafka-ui llevan ese sufijo (por ejemplo `billing-svc-server`).
5. **Desarrollo sin tsx**: `pnpm dev` en activation-api compila con `tsc` + `node --watch` en vez de `tsx watch`, porque esbuild no emite `design:paramtypes` en métodos con decoradores de parámetro y `@nestjs/swagger` crasheaba al arrancar.
6. **NotificationService escribe `notifications` solo si el email efectivamente se envió** (dentro de la tx de idempotencia).
7. **El primer `ActivationRequested` se emite por WebSocket desde el POST** (está en el history antes de que la UI se suscriba; la UI siembra la timeline con `GET /activations/:id`).
8. **Mongo en replica set de un nodo con healthcheck que inicializa `rs0` automáticamente** (necesario para las transacciones del helper de idempotencia).
9. **CRM analytics expone HTTP en 3004** (el spec no le asignaba puerto; el endpoint `GET /events` pedido en el paso 10 lo requería).
10. **loyalty-service** aparece solo con el profile `loyalty` de Compose y no persiste nada: solo loguea (escenario 5).
11. **Página de prueba WS** en `apps/activation-api/test/ws-test.html` (no parte de la UI final, pero útil para debug).

## 11. Fuera de alcance (del spec, confirmado)

Integración con sistemas reales (BSS, checkout), auth, alta disponibilidad, carga, Schema Registry, trazas, outbox transaccional (fase 4).
