# EventPipe

Event-driven pipeline — Java 21, Spring Boot, RabbitMQ, live SSE status page.

[![CI](https://github.com/alfauzan003/eventpipe/actions/workflows/ci.yml/badge.svg)](https://github.com/alfauzan003/eventpipe/actions/workflows/ci.yml)

## Tech stack

- Java 21
- Spring Boot 3.3
- RabbitMQ (AMQP)
- React + TypeScript (Vite)
- GitHub Actions (CI)

## Architecture

```
                 POST /api/events
                        |
                        v
                [EventPublisher]  (eventId + timestamp assigned)
                        |
              routing key = event type
                        |
                        v
        +---------------------------------+
        |  topic exchange eventpipe.events|
        +---------------------------------+
                        |
                        v
        +---------------------------------+
        |  queue eventpipe.events         |
        |  (x-dead-letter-exchange,       |         SSE: GET /api/events/stream
        |   x-dead-letter-routing-key)    |<-----------------------+
        +---------------------------------+                        |
                        |                                          |
                        v                                          |
              [EventConsumer] --broadcast on success--> [EventStreamService]
                        |                                          |
                        |  throws (processing failure)            v
                        v                               connected browsers
              retry x3 with exponential backoff          (EventSource)
              (1s, 2s, capped at 10s)
                        |
                        v  (attempts exhausted)
              reject without requeue
                        |
                        v
        +---------------------------------+
        |  DLX eventpipe.events.dlx       |
        +---------------------------------+
                        |
                        v
        +---------------------------------+
        |  DLQ eventpipe.events.dlq       |
        +---------------------------------+
```

Producer → topic exchange → queue → consumer. Failures are retried with
backoff, and poison events (which keep throwing) are dead-lettered to the DLQ
instead of blocking the queue forever.

## How to run

Prerequisites: JDK 21+, Maven, Node.js, and a running RabbitMQ (Docker works):

```sh
docker run -d --name eventpipe-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-alpine
```

**Backend** (from `backend/`):

```sh
mvn spring-boot:run
```

RabbitMQ connection settings are overridable via the `RABBITMQ_HOST`,
`RABBITMQ_PORT`, `RABBITMQ_USERNAME` and `RABBITMQ_PASSWORD` environment
variables. The server listens on port `8080`.

**Frontend** (from `frontend/`, in a second terminal):

```sh
npm ci
npm run dev
```

The Vite dev server proxies `/api` to `http://localhost:8080`. Open
`http://localhost:5173` to see the live status page: current counts, a publish
form, and events appearing in real time as they are processed.

## Event pipeline (backend)

### Publisher API

`POST /api/events` accepts an event, validates it, and publishes it to RabbitMQ:

```json
{ "type": "order.created", "payload": { "orderId": 42 } }
```

- `type` — required, non-blank; used as the publish routing key
- `payload` — required; arbitrary JSON

The server assigns `eventId` (UUID) and `timestamp` (now, UTC) and responds `202 Accepted` with the full envelope:

```json
{
  "eventId": "9f1c2b6e-4a3d-4c5e-8f90-123456789abc",
  "type": "order.created",
  "payload": { "orderId": 42 },
  "timestamp": "2026-08-16T12:00:00Z"
}
```

Validation failures (blank `type` or missing `payload`) return `400 Bad Request`.

### Envelope

Every event travels as an `EventEnvelope` record: `(eventId: UUID, type: String, payload: Object, timestamp: Instant)`.

### Topology

- Durable topic exchange `eventpipe.events`
- Durable queue `eventpipe.events` bound to it with `#` (routing key on publish = event `type`), with dead-letter args pointing at the DLX
- Durable topic exchange `eventpipe.events.dlx` + durable queue `eventpipe.events.dlq` (bound with routing key `eventpipe.events.dlq`)

Spring Boot's `RabbitAdmin` auto-declares the exchanges, queues and bindings on
startup. Names are configurable via `eventpipe.*` properties.

### Consumer, retry and dead-lettering

A `@RabbitListener` on `eventpipe.events` consumes with **manual acks**
(`spring.rabbitmq.listener.simple.acknowledge-mode=manual`): `basicAck` after
successful processing. Processing is **idempotent**: seen `eventId`s are tracked
in a thread-safe in-memory store, so a duplicate event is acked and skipped —
never processed twice.

Resilience is wired on a custom `SimpleRabbitListenerContainerFactory` in
`RabbitConfig` (the factory replaces the auto-configured one, so the
`spring.rabbitmq.listener.simple.retry.*` property route is not used):

- **Retry with backoff** — every listener is wrapped in a stateless
  `RetryInterceptorBuilder` with `max-attempts=3` and exponential backoff
  (`initial-interval=1000ms`, `multiplier=2.0`, `max-interval=10000ms`). A
  consumer that throws on processing is retried on the same delivery; a
  transient failure that succeeds on a later attempt is processed exactly once.
- **Dead-lettering** — when the attempts are exhausted, a custom
  `ManualAckRejectRecoverer` throws a top-level `AmqpRejectAndDontRequeueException`
  with `rejectManual=true`, which makes the manual-ack container nack the
  message without requeue. The main queue's `x-dead-letter-exchange` /
  `x-dead-letter-routing-key` args then route it to `eventpipe.events.dlq`. The
  queue keeps flowing: a good event published after a poison event is still
  processed. `defaultRequeueRejected=false` guarantees nothing is ever requeued
  forever. (The stock `RejectAndDontRequeueRecoverer` wraps the rejection in a
  `ListenerExecutionFailedException`, which the manual-ack container does not
  nack — hence the custom recoverer.)

Inspect the DLQ with the management UI (`http://localhost:15672`, guest/guest)
or `rabbitmqadmin`, then re-publish or purge as appropriate.

### Live feed (SSE)

`GET /api/events/stream` opens a long-lived `text/event-stream` connection.
Every successfully processed event is broadcast to all open emitters by
`EventStreamService` as an `event.processed` frame with the full envelope as
JSON. Each client gets its own `SseEmitter` (no timeout — the stream stays open
until the client disconnects); emitters are removed on completion, timeout or
error, so they never leak, and broadcasting is safe under concurrent consumers
(`CopyOnWriteArrayList` + thread-safe emitters).

### Observability

- `GET /api/events/counts` → `{ "totalProcessed": n, "byType": { "<type>": n, ... } }`
- `GET /api/events/last?limit=10` → most recently processed envelopes, newest first
- `GET /api/events/stream` → SSE live feed of processed events
- `GET /api/health` → `{ "status": "ok" }`
- Every publish and consume is logged

## Tests

Backend tests run against a real RabbitMQ broker via Testcontainers (Docker
required):

```sh
cd backend
mvn -B test
```

- `RabbitMQIntegrationTest` — publish→consume round trip over the real broker, duplicate `eventId` idempotency, SSE stream delivers processed events
- `RetryAndDlqIntegrationTest` — retry-then-success is processed exactly once; a poison event lands in the DLQ after retries and the consumer stays unblocked
- `EventControllerTest` / `EventControllerSseTest` / `HealthControllerTest` — REST + SSE endpoint slices
- `EventStreamServiceTest` — emitter registration and cleanup

Frontend:

```sh
cd frontend
npm ci
npm run build
```

## License

MIT — see [LICENSE](LICENSE).
