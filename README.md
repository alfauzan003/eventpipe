# EventPipe

Event-driven pipeline — Java 21, Spring Boot, RabbitMQ, live SSE status page.

[![CI](https://github.com/alfauzan003/eventpipe/actions/workflows/ci.yml/badge.svg)](https://github.com/alfauzan003/eventpipe/actions/workflows/ci.yml)

## Tech stack

- Java 21
- Spring Boot 3.3
- RabbitMQ (AMQP)
- React + TypeScript (Vite)
- GitHub Actions (CI)

## How to run

Placeholder — full instructions coming soon.

- **Backend**: `mvn spring-boot:run` from `backend/`
- **Frontend**: `npm install && npm run dev` from `frontend/`

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
- Durable queue `eventpipe.events` bound to it with `#` (routing key on publish = event `type`)

Spring Boot's `RabbitAdmin` auto-declares the exchange, queue and binding on startup.

### Consumer

A `@RabbitListener` on `eventpipe.events` consumes with **manual acks** (`spring.rabbitmq.listener.simple.acknowledge-mode=manual`):
`basicAck` after successful processing, `basicNack` (requeue) on failure. Processing is **idempotent**: seen `eventId`s
are tracked in a thread-safe in-memory store, so a duplicate event is acked and skipped — never processed twice.

### Observability

- `GET /api/events/counts` → `{ "totalProcessed": n, "byType": { "<type>": n, ... } }`
- `GET /api/events/last?limit=10` → most recently processed envelopes, newest first
- Every publish and consume is logged

RabbitMQ connection settings (`host`, `port`, `username`, `password`) are overridable via the `RABBITMQ_HOST`,
`RABBITMQ_PORT`, `RABBITMQ_USERNAME` and `RABBITMQ_PASSWORD` environment variables.

## License

MIT — see [LICENSE](LICENSE).
