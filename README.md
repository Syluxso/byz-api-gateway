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
| `/compact/**` | `BYZ_COMPACT_URI` | `http://127.0.0.1:8103` |

StripPrefix=1 (same as Java). Local health: `GET /actuator/health` → `{"status":"UP"}`.

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

Disable with `BYZ_KAFKA_ENABLED=false`.

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
