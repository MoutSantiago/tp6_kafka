# Guión de demo — POC Activación de Servicios con Kafka

Cinco escenarios que cubren los conceptos de mensajería de la POC.

## Preparación

```bash
# Restaurar un estado limpio (opcional pero recomendado si ya hubo corridas previas)
docker compose down -v
docker compose up -d --build
docker compose ps
```

Servicios visibles: KafkaUI en http://localhost:8080, Mailhog en http://localhost:8025, la demo UI en http://localhost:5173, y los docs Swagger de la API en http://localhost:3000/docs.

---

## Escenarios

### 1 — Camino feliz (fan-out)

Un solo evento `ActivationRequested` dispara billing y provisioning en paralelo y la saga cierra en `ACTIVE`.

```bash
curl -X POST http://localhost:3000/activations \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"C-demo-1","planId":"PLAN DIGITAL"}'
# Guardar el activationId devuelto para consultar el estado
sleep 8
curl http://localhost:3000/activations/<activationId>
```

**Qué mirar:**
- La terminal devuelve `{"activationId":"act-...","status":"PENDING"}` y las dos subsiguientes prospectivas se hacen invisibles al usuario — todos los cambios llegan por Kafka.
- **UI (5173)**: la timeline en vivo va `ActivationRequested → BillingAccountCreated → ProvisioningCompleted → ActivationCompleted`, todo en verde.
- **3000** (`curl` el GET): `status: ACTIVE`, los dos steps `OK`.
- **Kafka UI (8080)**: en `activation.requested`, `billing.events`, `provisioning.events` y `activation.events` aparece exactamente un mensaje por evento, todos con el mismo `correlationId`.
- **Mailhog (8025)**: email "Tu activación está lista" enviado a `C-demo-1@demo.local`.

### 2 — Fallo simulado y compensación

`simulateFailure: provisioning` hace que provisioning publique `ProvisioningFailed`; la saga termina en `FAILED` y billing anula la cuenta que ya había creado.

```bash
curl -X POST http://localhost:3000/activations \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"C-demo-2","planId":"PLAN PREMIUM","simulateFailure":"provisioning"}'
sleep 8
curl http://localhost:3000/activations/<activationId>
```

**Qué mirar:**
- **UI (5173)**: la timeline muestra `BillingAccountCreated` en verde, `ProvisioningFailed` y `ActivationFailed` en rojo y después `BillingAccountCancelled` en rojo.
- **3000**: `status: FAILED`, `steps.billing.status: OK`, `steps.provisioning.status: FAILED` con la razón.
- **Mongo**: la cuenta billing que se había creado queda en `CANCELLED` con `cancelReason`:
  ```bash
  docker exec tp6_kafka-mongo-1 sh -c 'mongosh --quiet billing_db --eval "db.billing_accounts.find({customerId: \"C-demo-2\"}).forEach(d => print(JSON.stringify(d)))"'
  ```
- **Mailhog (8025)**: email "No pudimos completar tu activación", indicando el paso que falló.

### 3 — Servicio caído: offsets y ningún mensaje perdido

`notification-service` tiene un `groupId` fijo (`notification-svc`) y commitea el offset solo después de procesar. Dejarla apagada, generar eventos, y al volver llegan todos.

```bash
docker compose stop notification-service
# Crear 3 activaciones mientras está caída (una con fallo para variar):
for i in a b; do
  curl -s -X POST http://localhost:3000/activations \
    -H 'Content-Type: application/json' -d "{\"customerId\":\"C-demo-3-$i\",\"planId\":\"PLAN DIGITAL\"}" > /dev/null
done
curl -s -X POST http://localhost:3000/activations \
  -H 'Content-Type: application/json' -d '{"customerId":"C-demo-3-c","planId":"PLAN DIGITAL","simulateFailure":"billing"}' > /dev/null
sleep 5

# El lag del consumer group muestra 3 mensajes sin consumir:
docker exec tp6_kafka-kafka-1 sh -c \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notification-svc-server'

# Al reencender, los tres emails llegan sin intervención:
docker compose start notification-service
sleep 15

# Y en Mongo hay un registro por cada uno (sin duplicados):
docker exec tp6_kafka-mongo-1 sh -c 'mongosh --quiet notification_db --eval "db.notifications.find({activationId: /demo-3/}).forEach(d => print(JSON.stringify(d)))"'
```

**Qué mirar:**
- **Kafka UI (8080)** o el comando de arriba: el consumer group `notification-svc-server` acumula **LAG = 3** mientras el servicio está caído.
- **Mailhog (8025)**: tras `start`, llegan los 3 emails (2 éxito + 1 error) — nadie pierde mensajes.
- **Los offsets** comparten commit por partición, no por servicio: en el `describe`, cada partición tiene `CURRENT-OFFSET` = `LOG-END-OFFSET` después de recuperarse.

### 4 — Escalado horizontal (particiones y consumer groups)

Dos réplicas de billing conviven en el mismo `consumer group billing-svc`: Kafka reparte las 3 particiones entre ellas; cada mensaje se procesa una única vez gracias a la clave de particionado y la idempotencia.

```bash
docker compose up -d --scale billing-service=2
sleep 20
# El grupo ahora tiene dos miembros en las 3 particiones de activation.requested:
docker exec tp6_kafka-kafka-1 sh -c \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group billing-svc-server --members'

# 6 activaciones = 6 cuentas, repartidas entre las réplicas:
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:3000/activations \
    -H 'Content-Type: application/json' -d "{\"customerId\":\"C-demo-4-$i\",\"planId\":\"PLAN DIGITAL\"}" > /dev/null
done
sleep 10
docker exec tp6_kafka-mongo-1 sh -c 'mongosh --quiet billing_db --eval "print(db.billing_accounts.countDocuments({customerId: /C-demo-4/}))"'
# Logs por réplica (algunas procesa una, otras la otra — pero una cuenta por activación):
docker logs tp6_kafka-billing-service-1 2>&1 | grep -c "C-demo-4"
docker logs tp6_kafka-billing-service-2 2>&1 | grep -c "C-demo-4"
```

**Qué mirar:**
- `--describe --members` muestra 2 consumidores miembros del mismo `billing-svc-server`.
- 6 cuentas creadas para 6 activaciones (no 12): el índice único por `activationId` y la idempotencia hacen el resto.
- Cada réplica procesa solo las particiones que le asigna Kafka; las dos procesan en paralelo y ningún mensaje se consume dos veces.

### 5 — Consumidor nuevo con auto.offset.reset=earliest (loyalty)

`loyalty-service` es un consumidor mínimo con su propio grupo (`loyalty-svc`) que solo loguea cada evento. Arranca con `fromBeginning: true`, así que al levantarse consume TODO el historial que ya está en los 4 topics.

```bash
# Lo esencial es que el servicio NO existía cuando se generaron los eventos anteriores.
docker compose --profile loyalty up -d
sleep 30

# El servicio nuevo lee desde el inicio:
docker logs tp6_kafka-loyalty-service-1 | grep "loyalty]" | head -20
# Debe haber muchos más eventos que los generados recién: está leyendo la historia completa.
docker logs tp6_kafka-loyalty-service-1 | grep -c "loyalty]"
```

**Qué mirar:**
- Los logs `loyalty] …` muestran eventos de horas/días anteriores (incluidos los de los escenarios 1–4), en orden por topic y partición.
- KafkaJS/consumer group nuevo (`loyalty-svc-server`) con los offsets empezando en 0 → earliest.
- Como baja de los 4 topics de "lado a lado": es un consumer group independiente, su consumo no molesta a billing/provisioning/notification/crm-analytics.

---

## Limpieza

```bash
docker compose --profile loyalty down
```
