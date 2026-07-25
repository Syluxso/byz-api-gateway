# Deploy byz-api-gateway (Go) on Linode

Replaces the Java `byz-gateway` JVM on port **8096**. Browsers keep calling `https://api.byzantineapp.dev` — no Admin/Hamlet URL changes.

## Env

```bash
export PORT=8096
export BIND=127.0.0.1
export REDIS_HOST=127.0.0.1
export REDIS_PORT=6379
# REDIS_PASSWORD unset for local Redis
export RATE_LIMIT_REPLENISH=40
export RATE_LIMIT_BURST=80
export KAFKA_BOOTSTRAP=127.0.0.1:9092
export BYZ_KAFKA_ENABLED=true
export BYZ_IAM_URI=http://127.0.0.1:8082
export BYZ_NOTIFICATIONS_URI=http://127.0.0.1:8081
export BYZ_DIRECTORY_URI=http://127.0.0.1:8086
export BYZ_EVENTS_URI=http://127.0.0.1:8088
export BYZ_FILES_URI=http://127.0.0.1:8089
```

| Item | Value |
|------|--------|
| Deploy dir | `/opt/services/byz-api-gateway` |
| Binary | `byz-api-gateway` |
| Supervisor | `byz-api-gateway` (stop legacy `byz-gateway` / Java name if present) |

## Cutover

1. Ensure Redis is up on loopback.
2. Deploy Go binary (Jenkinsfile).
3. Stop Java gateway on `:8096`.
4. Start Go; nginx `api.byzantineapp.dev` → `127.0.0.1:8096` unchanged.
5. Check health + CORS + one proxied call + Admin live gateway feed.

```bash
curl -s http://127.0.0.1:8096/actuator/health
curl -s https://api.byzantineapp.dev/iam/actuator/health
```
