# FALLBACK_HOSTING.md — eventpipe

**When this applies:** the four-project portfolio currently runs on a paid VPS
(1 core / 2 GB RAM, Docker Compose + Caddy for HTTPS). When that VPS year
ends, this document is the migration path for **eventpipe** (and its Java
pair, assetrack) onto free or near-free hosting.

## Current production setup (one paragraph)

Eventpipe is one Spring Boot 3 (Java 21) service that both serves the React UI
and exposes the publish API / SSE status feed on a single origin (`:8080`).
The frontend is built in a `node:20-alpine` stage and embedded into the jar as
static resources, so there is exactly **one** web process. It talks to
**RabbitMQ** for the event pipeline; it does **not** use any database
(processed events are held in memory, last 20 by default). All RabbitMQ
settings are environment variables (`RABBITMQ_HOST`, `RABBITMQ_PORT`,
`RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD`).

Nothing about the app is tied to the VPS, which is what makes the move cheap.

## The exit path in one paragraph

Eventpipe moves to **Render** (a web service built from the repo's
`Dockerfile`) plus **CloudAMQP Little Lemur** for RabbitMQ. Because the app
uses no Postgres, **no Neon database is needed** — that part of the stack
simply disappears. The Java pair (assetrack + eventpipe) goes to Render while
the C# pair (logiflow + factoryline) moves to Azure App Service F1 + Azure SQL
free — see the sibling `FALLBACK_HOSTING.md` files in those repos. There are
no cross-pair calls in this stack, so the split is fully independent.

## Option A (recommended): Render web service + CloudAMQP Little Lemur

### Why this fits eventpipe

- Render deploys straight from the repo's `Dockerfile` (free tier: 750
  instance-hours/month, spins down after 15 min idle, cold start ~30–60 s).
- CloudAMQP's free "Little Lemur" plan gives you a managed RabbitMQ instance
  (1 million messages/month — plenty for a demo pipeline), so you do not need
  to run or patch a RabbitMQ server yourself.
- The Dockerfile already pins `JAVA_TOOL_OPTIONS=-Xmx192m`, which is well
  under Render's free-tier memory limit.

### What to do

1. **CloudAMQP:** sign up, create a free "Little Lemur" instance, and copy the
   connection details (host, port, username, password) from the dashboard.
2. **Render web service:** create a new Web Service, connect the eventpipe
   repo, and choose "Docker" as the environment — Render builds the
   `Dockerfile` (it runs `npm ci && npm run build`, then
   `mvn -B -q -DskipTests package`, then serves `app.jar`).
3. **Env vars** on the Render service (same names the compose file used):
   | Env var | Value |
   |---|---|
   | `RABBITMQ_HOST` | CloudAMQP host (e.g. `rattlesnake-01.rmq.cloudamqp.com`) |
   | `RABBITMQ_PORT` | `5672` (or `5671` for TLS — see below) |
   | `RABBITMQ_USERNAME` | the CloudAMQP user |
   | `RABBITMQ_PASSWORD` | the CloudAMQP password |
   | `JAVA_TOOL_OPTIONS` | already set in the Dockerfile; override only if needed |
4. **TLS note:** CloudAMQP's free instances are reachable over plain AMQP
   (`amqp://`, port 5672) from Render's public internet. If you want an
   encrypted connection, use port 5671 and set `SPRING_RABBITMQ_SSL_ENABLED=true`
   (`spring.rabbitmq.ssl.enabled` is read as `SPRING_RABBITMQ_SSL_ENABLED`).
5. **Verify:** open the service URL — you should see the SSE status page;
   publish a test event and confirm it appears in the feed.

## Option B: keep it containerized elsewhere

Because the image is self-contained, any Docker host works unchanged:

- A cheap single VPS (the current setup, minus Caddy — Render gives you TLS).
- A free-tier cloud container service (e.g. Fly.io, Google Cloud Run, Azure
  Container Apps) — each maps the same `RABBITMQ_*` env vars. The only
  requirement is outbound access to your RabbitMQ endpoint on port 5672/5671.

## Config reference (works identically on the VPS, Render, or any Docker host)

| Env var | Purpose | Current value (VPS) | After migration |
|---|---|---|---|
| `RABBITMQ_HOST` | RabbitMQ hostname | `rabbitmq` (compose service) | CloudAMQP hostname |
| `RABBITMQ_PORT` | AMQP port | `5672` (app default) | `5672` or `5671` |
| `RABBITMQ_USERNAME` | AMQP user | from `.env` | CloudAMQP user |
| `RABBITMQ_PASSWORD` | AMQP password | from `.env` | CloudAMQP password |
| `JAVA_TOOL_OPTIONS` | JVM heap cap | `-Xmx192m` (Dockerfile ENV) | unchanged |

The app defaults to `localhost` / `guest` when env vars are absent, so a
missing variable degrades to a local-only setup instead of crashing.

## No Postgres here — and Neon anyway (for the sibling repo)

Eventpipe never touches a database, so the "Neon free Postgres" step that the
assetrack fallback doc describes does **not** apply to this service. If the
app ever gains persistence, Neon's free tier (0.5 GB storage) is the natural
fit and requires no code changes — just a `SPRING_DATASOURCE_URL` style env
var. Until then, leave the database out of the stack entirely.

## The split-stack plan (portfolio-wide)

- **Java pair (assetrack, eventpipe):** Render web services from their
  Dockerfiles; assetrack adds Neon free Postgres, eventpipe adds CloudAMQP
  free RabbitMQ.
- **C# pair (logiflow, factoryline):** Azure App Service F1 + Azure SQL free.
- No shared network is required: each pair only talks to its own managed
  services over standard ports.

## Migration checklist

- [ ] Create CloudAMQP Little Lemur instance; save host/port/user/password
- [ ] Create Render web service from the repo (Docker environment)
- [ ] Set `RABBITMQ_HOST` / `RABBITMQ_PORT` / `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD`
- [ ] (Optional) enable TLS: port 5671 + `SPRING_RABBITMQ_SSL_ENABLED=true`
- [ ] Attach your own domain if you want `eventpipe.yourdomain.com`
- [ ] Smoke-test: load the status page, publish an event, watch it stream via SSE
- [ ] Tear down the VPS RabbitMQ container once the new one is verified

## Gotchas

- **Free-tier spin-down:** Render's free web service sleeps after ~15 min
  without traffic. An open SSE connection counts as traffic, so an actively
  watched status page keeps the service warm; after idle, the first visitor
  waits ~30–60 s for the cold start.
- **Messages while the app is down:** events published to the queue while the
  Render service sleeps are consumed on wake-up (RabbitMQ buffers them) — but
  the in-memory "last 20 events" feed resets on restart, so history before the
  cold start is gone. That is acceptable for a demo.
- **CloudAMQP free caps:** 1M messages/month and a handful of queues. The
  dead-letter topology (exchange + queue + DLX + DLQ) fits comfortably.
- **JVM memory:** keep `-Xmx192m`; Render free instances give 512 MB, and the
  heap cap leaves room for Spring Boot's overhead.
