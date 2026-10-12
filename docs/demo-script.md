# Guión de demo — POC Activación de Servicios con Kafka

Cinco escenarios que cubren los conceptos de mensajería de la POC.

**Aclaración importante antes de empezar:** la UI (http://localhost:5173) solo muestra la timeline de la activación que creás **desde la propia UI** (al enviar el formulario, se suscribe por WebSocket a esa activación). Si creás la activación con `curl` desde la terminal, la UI no la conoce y no muestra nada sobre ella. Por eso cada escenario dice claramente qué hacer en cada lugar:

- **[Navegador]**: pestañas de la UI (5173), Kafka UI (8080), Mailhog (8025), Swagger (3000).
- **[Terminal]**: comandos a ejecutar, uno por uno. Después de cada comando se indica **qué mirar antes de seguir**.

---

## Preparación

**[Terminal] Comando 1** — borrar estado previo (volumenes incluidos):

```bash
docker compose down -v
```

> **Mirar:** no debe listar ningún servicio restante. Kafka y Mongo quedan limpios.

**[Terminal] Comando 2** — levantar todo:

```bash
docker compose up -d --build
```

> **Mirar:** la primera vez compila las imágenes (puede tardar varios minutos). Termina con todos los contenedores `Started`.

**[Terminal] Comando 3** — verificar el estado:

```bash
docker compose ps
```

> **Mirar:** `kafka`, `mongo` y `mailhog` con estado `healthy`; `activation-api`, `billing-service`, `provisioning-service`, `notification-service`, `crm-analytics-service`, `demo-ui` en `Up`. (Nota: `loyalty-service` NO aparece: solo levanta con el profile, escenario 5.)

**[Navegador]**: abrir estas cuatro pestañas y dejarlas listas:

| Pestaña | URL | Para qué |
| :--- | :--- | :--- |
| Demo UI | http://localhost:5173 | Formulario + timeline en vivo |
| Kafka UI | http://localhost:8080 | Topics, mensajes, consumer groups, lag |
| Mailhog | http://localhost:8025 | Emails enviados |
| Swagger | http://localhost:3000/docs | Referencia de la API |

---

## Escenarios

### 1 — Camino feliz (fan-out)

Un solo `ActivationRequested` hace reaccionar a billing y provisioning en paralelo; la saga cierra en `ACTIVE`.

#### Opción A — Todo desde la terminal

**[Terminal] Comando 1** — crear la activación:

```bash
curl -X POST http://localhost:3000/activations \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"C-demo-1","planId":"PLAN DIGITAL"}'
```

> **Mirar (terminal):** devuelve `{"activationId":"act-xxxxxxxx","status":"PENDING"}`. Guardá ese `activationId` para el siguiente comando.

**[Terminal] Comando 2** — esperar a los consumidores y consultar el estado final:

```bash
sleep 8 && curl http://localhost:3000/activations/act-xxxxxxxx
```

> **Mirar (terminal):** `"status": "ACTIVE"`, `steps.billing.status` y `steps.provisioning.status` en `"OK"`, y el `history` con los 4 eventos.

#### Opción B — Todo desde la UI

**[Navegador]** En http://localhost:5173:

1. **Cliente**: `C-demo-1-ui`, **Plan**: `PLAN DIGITAL`, **Forzar fallo**: `Ninguno` → clic en **Contratar plan**.
2. **Mirar:** la timeline se va poblando en vivo: `ActivationRequested`, `BillingAccountCreated`, `ProvisioningCompleted`, `ActivationCompleted`, todos con punto verde; el badge arriba a la derecha pasa de `PENDING` a `ACTIVE` (vía `IN_PROGRESS`).

#### Mirar el fan-out por fuera de la UI (ambas opciones)

**[Navegador] Kafka UI (8080)** → topics: en `activation.requested` hay 1 mensaje; en `billing.events` un `BillingAccountCreated`; en `provisioning.events` un `ProvisioningCompleted`; en `activation.events` un `ActivationCompleted`. Todos comparten el mismo `correlationId` (= el `act-xxx`).

**[Navegador] Mailhog (8025)**: 1 email, asunto "Tu activación está lista", para `C-demo-1@demo.local`.

> El mismo evento inicial (`ActivationRequested`) gatilló reacciones en 3 servicios: eso es el **fan-out**.

---

### 2 — Fallo simulado y compensación

Con `simulateFailure: "provisioning"`, provisioning publica `ProvisioningFailed` (fallo de negocio, no error técnico); la activación pasa a `FAILED` y billing anula la cuenta que ya había creado.

#### Opción A — Terminal

**[Terminal] Comando 1** — crear la activación con fallo:

```bash
curl -X POST http://localhost:3000/activations \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"C-demo-2","planId":"PLAN PREMIUM","simulateFailure":"provisioning"}'
```

> **Mirar (terminal):** devuelve un `activationId`. Guardalo.

**[Terminal] Comando 2** — esperar y consultar:

```bash
sleep 8 && curl http://localhost:3000/activations/act-xxxxxxxx
```

> **Mirar (terminal):** `"status": "FAILED"`, `steps.billing.status: "OK"`, `steps.provisioning.status: "FAILED"` con `reason`. El `history` incluye (en este orden): `ActivationRequested`, `BillingAccountCreated`, `ProvisioningFailed`, `ActivationFailed`, `BillingAccountCancelled`.

**[Terminal] Comando 3** — la compensación en Mongo:

```bash
docker exec tp6_kafka-mongo-1 sh -c 'mongosh --quiet billing_db --eval "db.billing_accounts.find({customerId: \"C-demo-2\"}).forEach(d => print(JSON.stringify(d)))"'
```

> **Mirar (terminal):** un documento con `"status": "CANCELLED"` y `"cancelReason": "Fallo simulado en provisioning"`. La cuenta existió y se anuló por el evento de fallo, sin que nadie llame directamente a billing.

#### Opción B — UI

**[Navegador]** En http://localhost:5173:

1. **Cliente**: `C-demo-2-ui`, **Plan**: `PLAN PREMIUM`, **Forzar fallo**: `Provisioning` → **Contratar plan**.
2. **Mirar:** la timeline muestra:
   - `ActivationRequested` → verde (es solo el disparador)
   - `BillingAccountCreated` → verde (éxito del paso billing)
   - `ProvisioningFailed` → rojo (fallo de negocio)
   - `ActivationFailed` → rojo (cierre de la saga)
   - `BillingAccountCancelled` → rojo (compensación)
   El badge queda en `FAILED`.
3. **Mirar (navegador, Mailhog en 8025):** email "No pudimos completar tu activación", con el paso (`provisioning`) y la razón.

> Concepto: **saga coreografiada** — la compensación ocurre por eventos; ningún servicio llamó a otro.

---

### 3 — Servicio caído: offsets y ningún mensaje perdido

`notification-service` consume con `groupId` fijo (`notification-svc`) y solo commitea el offset después de procesar. Lo detenemos, generamos eventos, y al reencender llegan todos.

**[Terminal] Comando 1** — detener la notificación:

```bash
docker compose stop notification-service
```

> **Mirar:** `docker compose ps` muestra `notification-service` sin estado running.

**[Terminal] Comando 2** — crear 3 activaciones con la notificación caída (2 felices, 1 con fallo en billing para tener un email de error):

```bash
for i in a b; do
  curl -s -X POST http://localhost:3000/activations \
    -H 'Content-Type: application/json' -d "{\"customerId\":\"C-demo-3-$i\",\"planId\":\"PLAN DIGITAL\"}" > /dev/null
done
curl -s -X POST http://localhost:3000/activations \
  -H 'Content-Type: application/json' -d '{"customerId":"C-demo-3-c","planId":"PLAN DIGITAL","simulateFailure":"billing"}' > /dev/null
```

> **Mirar:** no imprime nada visible, pero en este momento 3 mensajes quedaron sin consumir en `activation.events`. **[Navegador] Mailhog (8025):** todavía NO llegaron los emails de `C-demo-3-*`.

**[Terminal] Comando 3** — verificar el lag acumulado:

```bash
docker exec tp6_kafka-kafka-1 sh -c \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notification-svc-server'
```

> **Mirar (terminal):** las columnas `CURRENT-OFFSET` y `LOG-END-OFFSET` difieren: la suma de `LAG` de las 3 particiones es **3**. Esos mensajes no se perdieron: están en Kafka esperando al consumidor. (En Kafka UI: Consumers → `notification-svc-server` también muestra el lag.)

**[Terminal] Comando 4** — reencender el servicio y darle unos segundos:

```bash
docker compose start notification-service && sleep 15
```

> **[Navegador] Mirar (Mailhog, 8025):** tienen que llegar los 3 emails pendientes: 2 de `ActivationCompleted` ("está lista") para `C-demo-3-a` y `C-demo-3-b`, y 1 de `ActivationFailed` ("no pudimos completar") para `C-demo-3-c`.

**[Terminal] Comando 5** — verificar los registros en Mongo (ojo: el filtro es por `customerId`, porque el `activationId` se genera como `act-xxxx`):

```bash
docker exec tp6_kafka-mongo-1 sh -c 'mongosh --quiet notification_db --eval "db.notifications.find({customerId: /^C-demo-3-/}).forEach(d => print(d.activationId, d.eventType, d.to))"'
```

> **Mirar (terminal):** exactamente **3 documentos**, uno por activación, con el `eventType` correcto cada uno. (El índice único `(activationId, eventType)` garantiza que no haya duplicados si Kafka reentrega.)

**[Terminal] Comando 6 (opcional)** — verificar que el lag quedó en 0:

```bash
docker exec tp6_kafka-kafka-1 sh -c \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notification-svc-server'
```

> **Mirar (terminal):** en cada partición `CURRENT-OFFSET` = `LOG-END-OFFSET` (lag 0).

> Concepto: **offsets**. El group fijo conserva el punto de lectura; la caída del consumidor no pierde mensajes.

---

### 4 — Escalado horizontal (particiones y consumer groups)

Dos réplicas de `billing-service` comparten el mismo consumer group (`billing-svc`): Kafka reparte las 3 particiones entre ellas; cada mensaje se procesa una sola vez.

**[Terminal] Comando 1** — escalar billing a 2 réplicas:

```bash
docker compose up -d --scale billing-service=2
```

> **Mirar:** `docker compose ps | grep billing` muestra `tp6_kafka-billing-service-1` y `tp6_kafka-billing-service-2` en `Up`.

**[Terminal] Comando 2** — esperar a que re-balanceen y ver los miembros del grupo:

```bash
sleep 20
docker exec tp6_kafka-kafka-1 sh -c \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group billing-svc-server --members'
```

> **Mirar (terminal):** **2 filas** con distinto `CONSUMER-ID` pero mismo `GROUP` (`billing-svc-server`), repartiendo entre ellas las 3 particiones. **[Navegador] Kafka UI (8080)** → Consumers → `billing-svc-server` muestra el mismo reparto por partición.

**[Terminal] Comando 3** — generar 6 activaciones y esperar a que se procesen:

```bash
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:3000/activations \
    -H 'Content-Type: application/json' -d "{\"customerId\":\"C-demo-4-$i\",\"planId\":\"PLAN DIGITAL\"}" > /dev/null
done
sleep 10
```

> **Mirar:** nada visible aún; estamos saturando un poco para ver el reparto.

**[Terminal] Comando 4** — contar las cuentas creadas:

```bash
docker exec tp6_kafka-mongo-1 sh -c 'mongosh --quiet billing_db --eval "print(db.billing_accounts.countDocuments({customerId: /^C-demo-4-/}))"'
```

> **Mirar (terminal):** debe imprimir **`6`** — una cuenta por activación, no 12 (el procesamiento idempotente evita duplicados aunque Kafka entregue más de una vez).

**[Terminal] Comando 5** — ver el reparto entre réplicas:

```bash
docker logs tp6_kafka-billing-service-1 2>&1 | grep -c "C-demo-4"
docker logs tp6_kafka-billing-service-2 2>&1 | grep -c "C-demo-4"
```

> **Mirar (terminal):** dos números que suman **6**. No necesariamente 3/3: la clave del mensaje (`customerId`) define la partición, y las particiones se asignan por balanceo. La suma exacta muestra que ningún mensaje se procesó dos veces y ninguna réplica pisó a la otra.

> Concepto: **consumer group + particiones** — escalar réplicas aumenta el paralelismo hasta el máximo = cantidad de particiones del topic.

Para volver a una sola réplica al terminar el escenario:

```bash
docker compose up -d --scale billing-service=1
```

---

### 5 — Consumidor nuevo con auto.offset.reset=earliest (loyalty)

`loyalty-service` es un consumidor mínimo con grupo propio (`loyalty-svc`) que solo loguea cada evento. Arranca con `fromBeginning: true` (equivalente a `auto.offset.reset=earliest`), así que al levantarse por primera vez consume **todo el historial** que ya está en los 4 topics (incluidos los eventos de los escenarios anteriores).

**[Terminal] Comando 1** — levantar el servicio nuevo (solo se levanta con el profile, al que pertenece únicamente este servicio):

```bash
docker compose --profile loyalty up -d
```

> **Mirar:** aparece `tp6_kafka-loyalty-service-1` en `docker compose ps` — antes de este comando NO estaba, así que consumirá todos los eventos históricos.

**[Terminal] Comando 2** — darle tiempo a que consuma el backlog y mirar los logs:

```bash
sleep 30
docker logs tp6_kafka-loyalty-service-1 | grep "loyalty]" | head -20
```

> **Mirar (terminal):** muchas líneas `[loyalty] <EventType> (...) topic=... partition=... correlationId=act-...` — incluyendo eventos de minutos antes de que el servicio existiera.

**[Terminal] Comando 3** — contar cuántos eventos leyó:

```bash
docker logs tp6_kafka-loyalty-service-1 | grep -c "loyalty]"
```

> **Mirar (terminal):** un número grande (depende de cuánto se usó la POC antes; en un entorno recién preparado con los escenarios 1–4 son ~20; sin volumen borrado puede haber más de 100).

**[Navegador] Kafka UI (8080)** → Consumers: aparece el grupo `loyalty-svc-server` con offsets que arrancaron desde 0 (subieron rápido al leer el historial), sin afectar a los otros consumidores.

> Concepto: **replay del log** — el consumer group es lo que define "dónde voy leyendo"; un grupo nuevo puede leer todo de nuevo sin molestar al resto. Esto con APIs directas no es posible.

---

## Limpieza

```bash
docker compose --profile loyalty down
```

> Baja todos los servicios, incluido el profile "loyalty". El escalado (`--scale billing-service=2`) se normaliza en el siguiente `up` sin flags.
