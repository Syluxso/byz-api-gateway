# byz-api-gateway

Go edge for Byzantine — drop-in replacement for the Java `byz-gateway`.

**Path routing · Redis rate limit · X-Request-Id · CORS · Kafka access/usage events**

JWT validation stays on each backend.

| Setting | Default |
|---------|---------|
| HTTP | `8096` |
| Redis | `127.0.0.1:6379` |
| Kafka | `127.0.0.1:9092` |

## Public path map

| Gateway path | Backend env | Default |
|--------------|-------------|---------|
| `/iam/**` | `BYZ_IAM_URI` | `http://127.0.0.1:8082` |
| `/notifications/**` | `BYZ_NOTIFICATIONS_URI` | `http://127.0.0.1:8081` |
| `/directory/**` | `BYZ_DIRECTORY_URI` | `http://127.0.0.1:8086` |
| `/events/**` | `BYZ_EVENTS_URI` | `http://127.0.0.1:8088` |
| `/files/**` | `BYZ_FILES_URI` | `http://127.0.0.1:8089` |
| `/search/**` | `BYZ_SEARCH_URI` | `http://127.0.0.1:8099` |
| `/ingest/**` | `BYZ_INGEST_URI` | `http://127.0.0.1:8100` |
| `/chat/**` | `BYZ_CHAT_URI` | `http://127.0.0.1:8102` |
| `/agent/**` | `BYZ_AGENT_URI` | `http://127.0.0.1:8103` |
| `/compact/**` | `BYZ_AGENT_URI` | alias during cutover |
| `/fetch/**` | `BYZ_FETCH_URI` | `http://127.0.0.1:8104` |
| `/managed/**` | `BYZ_MANAGED_URI` | `http://127.0.0.1:8105` |
| `/kan/**` | `BYZ_KAN_URI` | `http://127.0.0.1:8109` |
| `/bb/**` | `BYZ_BB_URI` | `http://127.0.0.1:8110` |
| `/pay/**` | `BYZ_PAY_URI` | `http://127.0.0.1:8230` (byz-pay) |

Two `/pay` paths are not proxied:

- `POST /pay/webhooks/stripe/{configId}` is handled here: the gateway reads
  that config's signing secret from byz-pay (`/internal/v1/provider-configs/{id}/webhook-secret`,
  with its own client_credentials token from `GATEWAY_CLIENT_ID` /
  `GATEWAY_CLIENT_SECRET`, cached 5 minutes), checks `Stripe-Signature`
  over the raw body (5 minute window), publishes the raw body and signature
  to `byz.pay.stripe.webhook` (`KAFKA_TOPIC_PAY_STRIPE_WEBHOOK`) and answers
  200 only once Kafka accepted it. Not rate limited. Without the client
  credentials it answers 503. The gateway's client id must be in byz-pay's
  `PAY_INTERNAL_CLIENTS`.
- `/pay/internal/**` answers 404: byz-pay's internal routes are never public.

StripPrefix=1 (same as Java). Local health: `GET /actuator/health` → `{"status":"UP"}`.

Managed product API: `GET /managed/actuator/health` → byz-managed-api.  
Kanban: `GET /kan/healthz` → byz-kan.  
Builder Buddy: `GET /bb/healthz` → bb-api. JWT stays on bb-api.

Admin log tail (JWT via `IAM_JWKS_URL`): `GET /api/v1/admin/logs` — used by Admin Logs → API Gateway.

## Rate limit

Token bucket in Redis (Spring RedisRateLimiter-compatible script):

- replenish **40**/s, burst **80** (env `RATE_LIMIT_REPLENISH`, `RATE_LIMIT_BURST`)
- key = SHA-256 fingerprint of Bearer token, else client IP
- Redis errors **fail-open** (log + allow) so a Redis blip does not take down the API

OPTIONS and `/actuator/**` skip the limiter.

## CORS

- OPTIONS → **204** + ACAO reflecting `Origin` (credentials allowed)
- Proxied responses: strip upstream `Access-Control-*`, apply one gateway set
- Exposes `X-Request-Id`

## Kafka

| Topic | When | Key |
|-------|------|-----|
| `byz.gateway.access` | every non-actuator request | `X-Request-Id` |
| `byz.api.usage` | API-key JWT (`user_api_key` / `tenant_api_key`) | `tokenId` |

Disable with `BYZ_KAFKA_ENABLED=false`. Topics are created by byz-events, never by this service.

Events are published through `byz-platform/events`. The flat camelCase shape is
unchanged; the platform adds `producer`, `traceId` (same value as `requestId`),
and `tenantId` / `userId` (null on access events).

## Platform endpoints

- `GET /readyz` pings Redis and reports `DOWN` when the rate-limit backend is unreachable. `/healthz` and `/actuator/health` stay static liveness.
- `GET /metrics` Prometheus, including `byz_http_request_duration_seconds{route="proxy/<id>"}`.
- `GET /api/v1/admin/logs` last 500 log lines, JWT required.

## Run locally

```bash
# Redis + Kafka from projects/db
export KAFKA_BOOTSTRAP=127.0.0.1:9092
export REDIS_HOST=127.0.0.1
go run .
```

## Build

```bash
CGO_ENABLED=0 go build -o byz-api-gateway .
```

## Deploy

See [DEPLOY-LINODE.md](DEPLOY-LINODE.md). Stop Java `byz-gateway` on `:8096`, run this binary; leave `api.byzantineapp.dev` nginx unchanged.

## Verify after cutover

```bash
curl -si 'https://api.byzantineapp.dev/actuator/health' \
  -H 'Origin: https://sys.byzantineapp.dev'
# 200 + Access-Control-Allow-Origin: https://sys.byzantineapp.dev

curl -si -X OPTIONS 'https://api.byzantineapp.dev/notifications/api/v1/notifications' \
  -H 'Origin: https://claritasclassicalcommunity.org' \
  -H 'Access-Control-Request-Method: GET' \
  -H 'Access-Control-Request-Headers: authorization'
# 204 + single ACAO
```
