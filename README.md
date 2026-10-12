# POC Activación de Servicios con Kafka

Demo de activación de un plan de servicio orquestada por eventos en Kafka: fan-out a billing y provisioning, saga con compensación, idempotencia, reintentos + DLQ, y una timeline en vivo por WebSocket. Seis servicios NestJS, una UI en React, MongoDB y Mailhog.

Documentación:
- [`docs/architecture.md`](docs/architecture.md) — arquitectura real (con desvíos y decisiones).
- [`docs/demo-script.md`](docs/demo-script.md) — los 5 escenarios con comandos exactos y qué mirar en cada pantalla.

## Requisitos

- **Docker** y `docker compose` v2
- **Node.js** 20.10 o superior (hay un `package.json` con `engines.node: ">=20.10.0"` y pnpm vía corepack)
- **pnpm 9**: `corepack enable && corepack prepare pnpm@9.0.0 --activate` (o `npm i -g pnpm@9.0.0`)
- Puertos libres: 3000, 3004, 5173, 8080, 8025, 1025, 9092–9094, 27017

## Levantar todo (1 comando)

```bash
# Desde la raíz del repo:
docker compose up -d --build

# Estado de los servicios (activation-api, billing, provisioning, notification,
# crm-analytics, demo-ui, kafka, kafka-ui, mongo, mailhog):
docker compose ps
```

La primera vez, el build tarda unos minutos. El broker crea los 4 topics de negocio y sus DLQ automáticamente.

Para desarrollo local sin Docker para los servicios (manteniendo Kafka/Mongo/Mailhog en Docker):

```bash
docker compose up -d kafka kafka-init mongo mailhog kafka-ui
pnpm install
pnpm build

# En terminales separadas, en apps/<servicio>:
node dist/main.js                                # billing-service, provisioning-service, crm-analytics-service
SMTP_HOST=localhost node dist/main.js            # notification-service (SMTP en localhost)
node dist/main.js                                # activation-api (HTTP en 3000)
cd apps/demo-ui && pnpm dev                      # UI en 5173
```

## URLs y puertos

| Componente | URL / puerto | Para qué |
| :--- | :--- | :--- |
| Demo UI | http://localhost:5173 | Formulario y timeline en vivo |
| activation-api | http://localhost:3000 | `POST /activations`, `GET /activations/:id`, WebSocket |
| API docs (Swagger) | http://localhost:3000/docs | Explorador REST |
| crm-analytics | http://localhost:3004 | `GET /events?correlationId=<activationId>` |
| Kafka UI | http://localhost:8080 | Topics, mensajes, consumer groups, lag |
| Mailhog | http://localhost:8025 | Correos enviados (y el Smtp en 1025 para dev local) |
| MongoDB | mongodb://localhost:27017 | `billing_db`, `provisioning_db`, `activation_db`, etc. |
| Kafka (host) | localhost:9094 | consumidores/producers que corren fuera de Docker |
| Página de prueba WS | `apps/activation-api/test/ws-test.html` | Mínima, sin build, para debug del WebSocket |

## Los 5 escenarios (resumen — comandos completos en `docs/demo-script.md`)

| # | Escenario | Comando clave | Qué mirar |
| :--- | :--- | :--- | :--- |
| 1 | **Camino feliz** (fan-out) | `POST /activations` sin `simulateFailure` | Timeline 4 eventos en verde, email en Mailhog |
| 2 | **Fallo y compensación** | `POST /activations` con `"simulateFailure":"provisioning"` | `ProvisioningFailed` rojo, `BillingAccountCancelled`, email de error |
| 3 | **Servicio caído** | `docker compose stop notification-service` → 3 activaciones → `start` | Kafka UI: lag = 3; al reencender llegan los 3 emails |
| 4 | **Escalar** (consumer group) | `docker compose up -d --scale billing-service=2` | `--describe --members` reparte particiones; 1 cuenta por activación |
| 5 | **Consumidor nuevo** (replay del log) | `docker compose --profile loyalty up -d loyalty-service` | `loyalty-service` lee TODO el historial desde el inicio |

Utilidades:

```bash
# Republicar un evento ya procesado (idempotencia — no debe duplicar nada):
pnpm tsx scripts/replay-event.ts <eventId>

# Enviar mensaje inválido (reintentos 1s/2s/4s + DLQ en <topic>.dlq):
pnpm tsx scripts/send-poison.ts activation.requested broken
pnpm tsx scripts/send-poison.ts activation.events missing
```

## Estructura

Ver [`docs/architecture.md`](docs/architecture.md) — sección «Estructura del monorepo».

## Limpieza

```bash
docker compose --profile loyalty down    # baja todo (incluido el profile loyalty)
docker compose down -v                   # además borra volúmenes (datos de Kafka/Mongo)
```
