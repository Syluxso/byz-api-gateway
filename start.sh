#!/bin/bash
set -euo pipefail
export PORT="${PORT:-8096}"
export BIND="${BIND:-127.0.0.1}"
export REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
export REDIS_PORT="${REDIS_PORT:-6379}"
export RATE_LIMIT_REPLENISH="${RATE_LIMIT_REPLENISH:-40}"
export RATE_LIMIT_BURST="${RATE_LIMIT_BURST:-80}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
export BYZ_KAFKA_ENABLED="${BYZ_KAFKA_ENABLED:-true}"
export BYZ_IAM_URI="${BYZ_IAM_URI:-http://127.0.0.1:8082}"
export BYZ_NOTIFICATIONS_URI="${BYZ_NOTIFICATIONS_URI:-http://127.0.0.1:8081}"
export BYZ_DIRECTORY_URI="${BYZ_DIRECTORY_URI:-http://127.0.0.1:8086}"
export BYZ_EVENTS_URI="${BYZ_EVENTS_URI:-http://127.0.0.1:8088}"
export BYZ_FILES_URI="${BYZ_FILES_URI:-http://127.0.0.1:8089}"
export BYZ_SEARCH_URI="${BYZ_SEARCH_URI:-http://127.0.0.1:8099}"
export BYZ_INGEST_URI="${BYZ_INGEST_URI:-http://127.0.0.1:8100}"

exec /opt/services/byz-api-gateway/byz-api-gateway
