# FCyT — Sede Concepción del Uruguay
**Ingeniería en Sistemas de Información**  
**Sistemas Distribuidos - 2026**

---

# POC Activación de Servicios con Kafka
### Documento de arquitectura Kafka, NestJS y React

---

## 01 RESUMEN

La POC demuestra que la activación de un plan puede resolverse publicando un único evento en Kafka, sin que ningún servicio llame directamente a otro. Un cliente contrata un plan desde una UI en React; eso dispara facturación, aprovisionamiento, notificación y registro en CRM, coordinados por eventos.

---

## 02 Alcance

La POC corre completa en local con Docker Compose y simula los sistemas externos; no se integra con sistemas reales.

### Incluido
* **UI de demo** con formulario de contratación y timeline de eventos en vivo.
* **Seis servicios NestJS:** `activation-api`, `billing`, `provisioning`, `notification`, `crm-analytics` y `loyalty`.
* Kafka de un broker en modo **KRaft**, **Kafka UI** y **MongoDB**.
* Fallos forzados desde la UI, compensación, idempotencia, reintentos y **DLQ** (*Dead Letter Queue* - Cola de mensajes muertos).
* Guión de demo con cinco escenarios.

### Fuera de alcance
* Integración con BSS, checkout o proveedores reales.
* Autenticación y autorización de usuarios.
* Alta disponibilidad: múltiples brokers, réplicas y despliegue en la nube.
* Pruebas de carga y métricas de rendimiento.
* Schema Registry y trazas distribuidas (quedan como fase opcional).

---

## 03 Stack tecnológico

El stack prioriza levantar todo con un comando y hacer visible lo que pasa dentro de Kafka durante la demo.

| Componente | Tecnología | Por qué |
| :--- | :--- | :--- |
| **Mensajería** | Apache Kafka, modo KRaft, 1 broker | Sin ZooKeeper; un broker alcanza para demostrar particiones, offsets y consumer groups. |
| **Servicios** | NestJS + `@nestjs/microservices` (transporte Kafka) | `@EventPattern` y `ClientKafka` reducen el código de integración; estructura modular y tipada. |
| **Frontend** | React + Vite + `socket.io-client` | Arranque rápido; WebSocket para la timeline en vivo. |
| **Persistencia** | MongoDB | Documento flexible para el estado de la activación; soporta transacciones para el outbox. |
| **Observabilidad** | Kafka UI (provectuslabs/kafka-ui) | Muestra topics, mensajes, particiones y lag de consumidores en vivo. |
| **Email simulado** | Mailhog | Bandeja de entrada visible para las notificaciones. |
| **Entorno** | Docker Compose | Un solo comando para levantar todo. |
| **Monorepo** | Modo monorepo de Nest o pnpm workspaces | Contratos de eventos compartidos en una librería. |

> **Alternativa:** Redpanda, con Redpanda Console es más liviano y compatible con la API de Kafka, pero la demo pierde el valor de mostrar Kafka tal como se usaría en producción.

---

## 04 Arquitectura

`activation-api` es el único punto de entrada; todo lo demás reacciona a eventos en Kafka.

### Un solo evento en Kafka dispara toda la activación

```
+-----------------------------------------------------------+
|                      Demo UI - React                      |
|             Formulario y timeline en vivo                 |
+-----------------------------+-----------------------------+
               |              ^
  POST /activations            | WebSocket
               v              |
+-----------------------------+-----------------------------+       lee y escribe       +-----------------------+
|                 activation-api · NestJS                   |<------------------------->|        MongoDB        |
|           REST, WebSocket, estado de la saga              |                           | Activaciones, outbox, |
+-----------------------------+-----------------------------+                           |     idempotencia      |
                              |                                                         +-----------------------+
                              | publica ActivationRequested
                              v
+---------------------------------------------------------------------------------------------------------------+
|                                                Kafka · modo KRaft                                             |
|                                3 particiones por topic - clave = customerId                                   |
|  +-------------------------------------+                      +--------------------------------------------+  |
|  |        activation.requested         |                      |               billing.events               |  |
|  +-------------------------------------+                      +--------------------------------------------+  |
|  +-------------------------------------+                      +--------------------------------------------+  |
|  |         provisioning.events         |                      |             activation.events              |  |
|  +-------------------------------------+                      +--------------------------------------------+  |
+-------+--------------------+--------------------------------------+-----------------------------------+-------+
        | ^                  | ^                                    |                                   |
        | |                  | |                                    v                                   v
+-------v-+-------+  +-------v-+-------+                  +---------+---------+               +---------+---------+
|     billing     |  |   provisioning  |                  |   notification    |               |   crm-analytics   |
|  Crea y anula   |  | Aprovisiona la  |                  | Email de bienvenida|               | Solo escucha todo |
|     cuentas     |  |      línea      |                  +-------------------+               +-------------------+
+-----------------+  +-----------------+
  Flechas hacia arriba: resultados que billing y provisioning publican de vuelta en Kafka
```

*Figura: Arquitectura de la POC: 7 componentes, 4 topics.*

Billing y provisioning trabajan en paralelo sobre el mismo evento y publican su resultado; `activation-api` junta ambos y decide si la activación terminó bien o mal.

### Componentes

* **Demo UI (React + Vite):** Formulario "Contratar plan" con cliente, plan y un selector para forzar un fallo en billing o provisioning. Muestra la timeline de eventos de cada activación, recibida por WebSocket.
* **activation-api (NestJS):** Expone `POST /activations` y `GET /activations/:id`, guarda la activación y publica `ActivationRequested`. Consume los resultados de billing y provisioning, mantiene el estado de la saga y publica `ActivationCompleted` o `ActivationFailed`. Un `@WebSocketGateway` envía cada cambio a la UI.
* **billing-service:** Consume `ActivationRequested` y crea la cuenta de facturación simulada. Consume `ActivationFailed` para anular la cuenta (compensación).
* **provisioning-service:** Consume `ActivationRequested` y simula el aprovisionamiento con un retardo de 1 a 3 segundos. Puede fallar a pedido.
* **notification-service:** Consume `activation.events` y envía el email de bienvenida o de error a Mailhog.
* **crm-analytics-service:** Consume todos los topics y no publica nada. Demuestra que se puede sumar un consumidor sin tocar a los demás.
* **loyalty-service:** Consume todos los topics desde el inicio (`auto.offset.reset=earliest`) con un consumer group propio. Demuestra replay del log histórico sin afectar a los consumidores existentes. No publica eventos.
* **MongoDB:** Una base de datos por servicio: el estado de cada activación, el outbox y los eventos ya procesados.
* **Kafka UI:** Herramienta de apoyo para mostrar topics, mensajes y consumer groups durante la demo.

---

## 05 Requerimientos

Los requerimientos se limitan a lo que la demo necesita mostrar; los no funcionales apuntan a confiabilidad del flujo, no a rendimiento.

### Funcionales

| ID | Requerimiento |
| :--- | :--- |
| **RF-01** | La UI permite crear una activación indicando `customerId`, `planId` y un modo de fallo opcional (`ninguno`, `billing`, `provisioning`). |
| **RF-02** | `POST /activations` responde `202 Accepted` con el `activationId` y el estado `PENDING`, sin esperar a los demás servicios. |
| **RF-03** | `activation-api` publica `ActivationRequested` por cada activación creada. |
| **RF-04** | `billing` y `provisioning` procesan el mismo evento en paralelo y publican su resultado (éxito o fallo). |
| **RF-05** | Con `billing` y `provisioning` OK, la activación pasa a `ACTIVE` y se publica `ActivationCompleted`. |
| **RF-06** | Si alguno falla, la activación pasa a `FAILED`, se publica `ActivationFailed` y `billing` anula la cuenta si la había creado. |
| **RF-07** | `notification` envía un email por cada `ActivationCompleted` o `ActivationFailed`. |
| **RF-08** | La UI muestra cada evento de la activación en una timeline, en tiempo real, vía WebSocket. |
| **RF-09** | `GET /activations/:id` devuelve el estado actual y el historial de eventos. |
| **RF-10** | `crm-analytics` registra todos los eventos y puede reprocesar el historial desde el inicio. |

### No funcionales

| ID | Requerimiento |
| :--- | :--- |
| **RNF-01** | Todo el entorno se levanta con `docker compose up`. |
| **RNF-02** | Los eventos de un mismo cliente se procesan en orden: clave de partición = `customerId`. |
| **RNF-03** | Cada consumidor es idempotente: un evento repetido no produce efectos duplicados. |
| **RNF-04** | Un mensaje que falla 3 veces va a la DLQ y no bloquea la partición. |
| **RNF-05** | Si un servicio se cae y vuelve, procesa los eventos pendientes sin perder ninguno. |
| **RNF-06** | Todos los eventos llevan `correlationId` para rastrear una activación de punta a punta. |
| **RNF-07** | Los contratos de eventos viven en una librería compartida y tienen versión. |

---

## 06 Topic y Contratos de eventos

Cuatro topics de negocio más sus DLQ; todos con 3 particiones, retención de 7 días y `customerId` como clave.

| Topic | Eventos | Productor | Consumidores (consumer group) |
| :--- | :--- | :--- | :--- |
| `activation.requested` | `ActivationRequested` | `activation-api` | `billing-svc`, `provisioning-svc`, `crm-analytics` |
| `billing.events` | `BillingAccountCreated`, `BillingFailed`, `BillingAccountCancelled` | `billing` | `activation-api`, `crm-analytics` |
| `provisioning.events` | `ProvisioningCompleted`, `ProvisioningFailed` | `provisioning` | `activation-api`, `crm-analytics` |
| `activation.events` | `ActivationCompleted`, `ActivationFailed` | `activation-api` | `notification-svc`, `billing-svc`, `crm-analytics` |
| `<topic>.dlq` | Mensajes que fallaron 3 veces | El consumidor que falló | Inspección manual en Kafka UI |

Todos los eventos comparten el mismo sobre. El payload cambia según el tipo de evento.

```json
{
  "eventId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "eventType": "ActivationRequested",
  "version": 1,
  "occurredAt": "2026-09-27T16:40:00Z",
  "correlationId": "act-0001",
  "customerId": "C-1234",
  "source": "activation-api",
  "simulateFailure": "none",
  "payload": {
    "planId": "PLAN_DIGITAL",
    "channel": "web"
  }
}
```

| Campo | Uso |
| :--- | :--- |
| `eventId` | Identificador único; base de la idempotencia. |
| `eventType` + `version` | Permiten evolucionar el contrato sin romper consumidores. |
| `correlationId` | El `activationId`; agrupa todos los eventos de una activación en la timeline. |
| `customerId` | También es la clave del mensaje en Kafka; garantiza orden por cliente. |
| `source` | Servicio que publicó el evento. |
| `simulateFailure` | Solo para `ActivationRequested`: indica a billing o provisioning que deben fallar (`none` | `billing` | `provisioning`). |

**Convenciones:** Nombres de eventos en pasado (describen un hecho), topics en `dominio.tipo`, cambios compatibles suman campos opcionales y los incompatibles suben `version`.

**Versionado de eventos:**
- **Compatible (misma versión):** agregar campos opcionales en `payload`, nuevos eventos en topics existentes.
- **Incompatible (nueva versión):** renombrar/eliminar campos, cambiar tipos, cambiar semántica de campos existentes → incrementar `version` en el sobre y/o crear topic nuevo (`dominio.tipo.v2`).

---

## 07 Flujos

La activación termina bien solo si billing y provisioning confirman; cualquier fallo dispara la compensación en billing.

```
Demo UI         activation-api         Kafka            billing       provisioning      notification
   |                  |                  |                 |                |                 |
   | POST /activations|                  |                 |                |                 |
   |----------------->|                  |                 |                |                 |
   | 202 Accepted     |                  |                 |                |                 |
   |   (PENDING)      |                  |                 |                 |                 |
   |<-----------------|                  |                 |                |                 |
   |                  | ActivationRequested                |                |                 |
   |                  |----------------->|                 |                |                 |
   |                  |                  |===== [paralelo] ===================================|
   |                  |                  | ActivationRequested              |                 |
   |                  |                  |---------------->|                |                 |
   |                  |                  | BillingAccountCreated            |                 |
   |                  |                  |<----------------|                |                 |
   |                  |                  |----------------------------------+                 |
   |                  |                  | ActivationRequested              |                 |
   |                  |                  |--------------------------------->|                 |
   |                  |                  | ProvisioningCompleted / ProvisioningFailed         |
   |                  |                  |<---------------------------------|                 |
   |                  |                  |====================================================|
   |                  | resultados de billing y provisioning               |                 |
   |                  |<-----------------|                 |                |                 |
   |                  |===== [alt] ===========================================================|
   |                  |                  | [ambos OK]      |                |                 |
   |                  | ActivationCompleted                |                |                 |
   |                  |----------------->|                 |                |                 |
   |                  |                  | ActivationCompleted              |                 |
   |                  |                  |--------------------------------------------------->|
   |                  |                  |                 |                | email de bienvenida
   |                  |                  |                 |                |---------------->|
   |                  |                  |                 |                |<----------------|
   |                  |------------------+----------------------------------------------------|
   |                  |                  | [alguno falló]  |                |                 |
   |                  | ActivationFailed |                 |                |                 |
   |                  |----------------->|                 |                |                 |
   |                  |                  | ActivationFailed|                |                 |
   |                  |                  |---------------->|                |                 |
   |                  |                  | BillingAccountCancelled          |                 |
   |                  |                  |<----------------|                |                 |
   |                  |                  | ActivationFailed                 |                 |
   |                  |                  |--------------------------------------------------->|
   |                  |                  |                 |                | email de error  |
   |                  |                  |                 |                |---------------->|
   |                  |                  |                 |                |<----------------|
   |                  |=======================================================================|
   | cada cambio de estado por WebSocket |                 |                |                 |
   |<-----------------|                  |                 |                |                 |
```

*Figura: Secuencia de la activación: camino feliz y compensación.*

### Estados de la activación

| Estado | Cuándo | Siguiente |
| :--- | :--- | :--- |
| **PENDING** | Al crear la activación | IN_PROGRESS |
| **IN_PROGRESS** | Llegó el primer resultado de billing o provisioning | ACTIVE / FAILED |
| **ACTIVE** | Billing y provisioning confirmaron | final |
| **FAILED** | Alguno de los dos falló | final (con compensación) |

**Regla de agregación en `activation-api`:** Guarda el resultado de cada servicio en el documento de la activación. Con los dos OK publica `ActivationCompleted`; con el primer fallo publica `ActivationFailed` e ignora resultados posteriores de esa activación, salvo registrarlos en el historial.

> **Caso borde:** Si provisioning falla antes de que billing cree la cuenta, billing puede recibir `ActivationFailed` antes de terminar. Billing debe anular la cuenta también si la crea después de haber visto el fallo.

---

## 08 Modelo de datos (MongoDB)

Cada servicio tiene su propia **base de datos** (MongoDB database); ninguno lee la base de otro. Las colecciones `outbox` y `processed_events` existen **en cada base de datos** de los servicios que publican o consumen (replicadas por servicio, no compartidas).

| Colección | Servicio | Base de datos | Contenido | Índices |
| :--- | :--- | :--- | :--- | :--- |
| `activations` | `activation-api` | `activation_db` | Estado de la activación, resultados parciales e historial de eventos | `_id` = `activationId`; `customerId` |
| `billing_accounts` | `billing` | `billing_db` | Cuenta simulada por activación, con estado `CREATED` o `CANCELLED` | `activationId` único |
| `provisioning_orders` | `provisioning` | `provisioning_db` | Orden simulada de aprovisionamiento | `activationId` único |
| `notifications` | `notification` | `notification_db` | Emails enviados | `activationId` + `eventType` |
| `event_log` | `crm-analytics` | `crm_db` | Copia de cada evento recibido | `eventId` único; `correlationId` |
| `outbox` | `activation-api`, `billing`, `provisioning` | (cada una en su DB) | Eventos pendientes de publicar | `status` + `createdAt` |
| `processed_events` | Todos los consumidores | (cada una en su DB) | `eventId` ya procesados | `eventId` único; TTL de 7 días |

### Ejemplo de documento en `activations`:

```json
{
  "_id": "act-0001",
  "customerId": "C-1234",
  "planId": "PLAN DIGITAL",
  "status": "ACTIVE",
  "simulateFailure": "none",
  "steps": {
    "billing": { "status": "OK", "at": "2026-09-27T16:40:02Z" },
    "provisioning": { "status": "OK", "at": "2026-09-27T16:40:03Z" }
  },
  "history": [
    { "eventType": "ActivationRequested", "at": "2026-09-27T16:40:00Z" },
    { "eventType": "BillingAccountCreated", "at": "2026-09-27T16:40:02Z" },
    { "eventType": "ProvisioningCompleted", "at": "2026-09-27T16:40:03Z" },
    { "eventType": "ActivationCompleted", "at": "2026-09-27T16:40:03Z" }
  ],
  "createdAt": "2026-09-27T16:40:00Z",
  "updatedAt": "2026-09-27T16:40:03Z"
}
```

El TTL de `processed_events` coincide con la retención de los topics: un evento más viejo que eso ya no puede volver a llegar.

---

## 09 Decisiones técnicas

Cuatro decisiones hacen que el flujo sea confiable aunque los servicios se caigan o Kafka entregue un mensaje dos veces.

1. **Idempotencia en cada consumidor.** Kafka garantiza entrega *"al menos una vez"*: si un consumidor se cae después de procesar pero antes de confirmar el offset, recibe el mensaje de nuevo. Cada consumidor inserta el `eventId` en `processed_events` (índice único) en la misma transacción que su efecto. Si la inserción falla por duplicado, descarta el evento y confirma el offset.
2. **Reintentos y DLQ.** Ante un error, el consumidor reintenta 3 veces con espera creciente (1 s, 2 s, 4 s). Si sigue fallando, publica el mensaje original en `<topic>.dlq` con el motivo del error en los headers y continúa con el siguiente. Así un mensaje roto no frena al resto de la partición. Los errores de negocio simulados (`simulateFailure`) no son errores técnicos: se publican como `BillingFailed` o `ProvisioningFailed`, no van a la DLQ.
3. **Outbox transaccional (fase 4).** Sin outbox, si `activation-api` guarda en Mongo y se cae antes de publicar, el evento se pierde. Con outbox, la activación y el evento se guardan en la misma transacción de Mongo; un proceso aparte lee outbox cada 500 ms, publica los pendientes y los marca como enviados. Para las fases 1 a 3 se publica directo después de guardar y se documenta el riesgo.
4. **Cliente Kafka.** Se usa el transporte Kafka de `@nestjs/microservices` (`ClientKafka` para publicar, `@EventPattern` para consumir), que funciona sobre `kafkajs`. Alcanza para la POC. Para un piloto conviene revisar el estado de mantenimiento de `kafkajs` y evaluar el cliente oficial de Confluent para JavaScript.

### Otras decisiones:
* **Coreografía con agregador:** `billing` y `provisioning` reaccionan solos al evento; `activation-api` solo junta resultados. Es más simple que un orquestador que da órdenes y muestra mejor el desacople.
* **Commit de offset manual:** Se configura `autoCommit: false` en `ClientKafka` y se llama `context.getMessage().commitOffset()` (o `manualCommit()`) **después** de procesar exitosamente y persistir en `processed_events`. Nunca antes.
* Mongo en replica set de un nodo, necesario para usar transacciones.

---

## 10 Estructura del repo y entorno local

Un monorepo con cinco apps NestJS, la UI y una librería de contratos; Docker Compose levanta la infraestructura y las apps.

```text
activation-poc/
├── apps/
│   ├── activation-api/         # REST + WebSocket + agregador de la saga
│   ├── billing-service/
│   ├── provisioning-service/
│   ├── notification-service/
│   ├── crm-analytics-service/
│   ├── loyalty-service/
│   └── demo-ui/                # React + Vite + socket.io-client
├── libs/
│   ├── contracts/              # Tipos de eventos, sobre común, nombres de topics
│   └── kafka-toolkit/          # Idempotencia, reintentos, DLQ, outbox
├── docker/
│   ├── create-topics.sh        # Crea los topics con 3 particiones
│   └── docker-compose.yml
└── docs/
    ├── architecture.md
    └── demo-script.md
```

`libs/contracts` es la única fuente de verdad de los eventos. `libs/kafka-toolkit` evita repetir en cada servicio la lógica de idempotencia, reintentos y DLQ.

### Servicios de Docker Compose

| Servicio | Imagen o build | Puerto local |
| :--- | :--- | :--- |
| `kafka` | `apache/kafka:3.7.0` (KRaft, 1 nodo) | 9092 |
| `kafka-init` | `apache/kafka:3.7.0` + `create-topics.sh` | - |
| `kafka-ui` | `provectuslabs/kafka-ui:0.7.2` | 8080 |
| `mongo` | `mongo:7.0` (replica set de 1 nodo) | 27017 |
| `mailhog` | `mailhog/mailhog:v1.0.1` | 8025 |
| `activation-api` | build `apps/activation-api` | 3000 |
| `billing-service` | build `apps/billing-service` | - |
| `provisioning-service` | build `apps/provisioning-service` | - |
| `notification-service` | build `apps/notification-service` | - |
| `crm-analytics-service` | build `apps/crm-analytics-service` | - |
| `loyalty-service` | build `apps/loyalty-service` | - |
| `demo-ui` | build `apps/demo-ui` | 5173 |

Variables de entorno comunes: `KAFKA_BROKERS`, `KAFKA_CLIENT_ID`, `KAFKA_GROUP_ID`, `MONGO_URI`, `VITE_API_URL=http://localhost:3000`. Las versiones exactas de las imágenes se fijan al armar el `docker-compose.yml`.

---

## 11 Guión de la demo

Cinco escenarios, cada uno demuestra un concepto de Kafka. Pantalla dividida: Demo UI a la izquierda, Kafka UI a la derecha.

| # | Escenario | Qué se hace | Qué se ve | Concepto |
| :-: | :--- | :--- | :--- | :--- |
| **1** | **Camino feliz** | Contratar un plan sin fallo | Timeline con 4 eventos en verde; email en Mailhog | **Fan-out:** un evento, varias reacciones |
| **2** | **Fallo y compensación** | Contratar con fallo en provisioning | `ProvisioningFailed`, `ActivationFailed`, `BillingAccountCancelled`; email de error | **Saga sin llamadas directas** |
| **3** | **Servicio caído** | Detener `notification`, crear 3 activaciones, volver a levantarlo | Kafka UI muestra lag de 3; al volver, llegan los 3 emails | **Offset:** nadie pierde mensajes |
| **4** | **Escalar** | Levantar una segunda instancia de `billing` | Kafka UI reparte las 3 particiones entre 2 consumidores | **Consumer group** y particiones |
| **5** | **Nuevo consumidor** | Levantar un servicio loyalty con un group nuevo que lee desde el inicio | Procesa todo el historial sin tocar a los demás | **Replay del log** |

### Comandos de apoyo:

```bash
# Escenario 3
docker compose stop notification-service
docker compose start notification-service

# Escenario 4
docker compose up -d --scale billing-service=2

# Escenario 5 (Loyalty con auto.offset.reset=earliest)
docker compose --profile loyalty up -d loyalty-service
```

El escenario 5 cierra la demo: muestra algo que con APIs directas no se puede hacer.  
*Duración estimada: 15 minutos.*

---

## 12 Plan de fases, criterios de acceptance y riesgos

Cuatro fases incrementales; la fase 4 solo se hace si la POC avanza a piloto. Cada rombo es el criterio para cerrar la fase.

### La demo es presentable al cerrar la fase 2

```
+------------------------+      +------------------------+      +------------------------+      +------------------------+
|     Fase 1 · Base      |      |    Fase 2 · Visible    |      |    Fase 3 · Robusta    |      |   Fase 4 · Opcional    |
|   Infra en Compose     |      |   UI con timeline WS   |      |      Idempotencia      |      |  Outbox transaccional  |
|     activation-api     |----->|  notification, Mailhog |----->|    Reintentos y DLQ    |----->|    Schema Registry     |
|  billing, provisioning |      |    Fallos forzados     |      | crm-analytics, loyalty |      |     OpenTelemetry      |
|  Camino feliz con logs |      |      Compensación      |      |    Escenarios 3 a 5    |      |                        |
|  (publicación directa) |      |                        |      |                        |      |                        |
+-----------+------------+      +-----------+------------+      +-----------+------------+      +-----------+------------+
            |                               |                               |                               |
            v                               v                               v                               v
       [ RF-01 a ]                    [ Escenarios 1 y 2 ]             [ Escenarios 1 a 5 ]            [ Ningún evento  ]
       [ RF-05 OK ]                   [ demo presentable ]             [ sin duplicados   ]            [ perdido si cae ]
                                                                                                         [ la API         ]
```

*Nota: Fases 1-3 usan publicación directa tras guardar (riesgo de pérdida si cae la API antes de publicar). Fase 4 añade outbox transaccional para eliminar ese riesgo.*

*Figura: Plan por fases: 4 fases, 4 puntos de control.*

La fase 2 es la mínima para presentar; la fase 3 agrega los escenarios que muestran la resiliencia de Kafka.

### Criterios de aceptación de la POC

- [ ] `docker compose up` levanta todo sin pasos manuales.
- [ ] Los 5 escenarios del guión se ejecutan de punta a punta en menos de 15 minutos.
- [ ] Reenviar un evento ya procesado no genera cuentas ni emails duplicados.
- [ ] Un mensaje inválido termina en la DLQ y el resto del flujo sigue.
- [ ] Todos los eventos de una activación se pueden rastrear por `correlationId`.

### Riesgos

| Riesgo | Impacto | Mitigación |
| :--- | :--- | :--- |
| **Kafka tarda en arrancar y los servicios fallan al conectar** | La demo no arranca | `healthcheck` en Compose y `depends_on` con `condition: service_healthy` |
| **Rebalanceo lento al escalar billing (escenario 4)** | Pausa visible de varios segundos | Explicarlo como parte de la demo; ajustar `session.timeout.ms` |
| **Transacciones de Mongo requieren replica set** | El outbox y la idempotencia no funcionan | Mongo en replica set de un nodo desde la fase 1 (`--replSet rs0` + `rs.initiate()`) |
| **Mantenimiento limitado de kafkajs** | Deuda técnica si la POC pasa a piloto | Aislar el cliente en `libs/kafka-toolkit` para poder reemplazarlo |
| **La demo depende de la red local** | Fallo en vivo | Grabar un video de respaldo de los 5 escenarios |