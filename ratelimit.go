package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Spring Cloud Gateway RedisRateLimiter-compatible token bucket (seconds).
const rateLimitScript = `
local tokens_key = KEYS[1]
local timestamp_key = KEYS[2]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local fill_time = capacity / rate
local ttl = math.floor(fill_time * 2)
if ttl < 1 then
  ttl = 1
end

local last_tokens = tonumber(redis.call("get", tokens_key))
if last_tokens == nil then
  last_tokens = capacity
end

local last_refreshed = tonumber(redis.call("get", timestamp_key))
if last_refreshed == nil then
  last_refreshed = 0
end

local delta = math.max(0, now - last_refreshed)
local filled_tokens = math.min(capacity, last_tokens + (delta * rate))
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
if allowed then
  new_tokens = filled_tokens - requested
end

redis.call("setex", tokens_key, ttl, new_tokens)
redis.call("setex", timestamp_key, ttl, now)

if allowed then
  return 1
end
return 0
`

type RateLimiter struct {
	rdb      *redis.Client
	script   *redis.Script
	rate     int
	capacity int
}

func NewRateLimiter(rdb *redis.Client, replenishRate, burstCapacity int) *RateLimiter {
	if replenishRate < 1 {
		replenishRate = 40
	}
	if burstCapacity < 1 {
		burstCapacity = 80
	}
	return &RateLimiter{
		rdb:      rdb,
		script:   redis.NewScript(rateLimitScript),
		rate:     replenishRate,
		capacity: burstCapacity,
	}
}

func rateLimitKey(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if strings.TrimSpace(auth) != "" {
		token := auth
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			token = strings.TrimSpace(auth[7:])
		}
		sum := sha256.Sum256([]byte(token))
		return "token:" + hex.EncodeToString(sum[:8])
	}
	ip := clientIP(r)
	return "ip:" + ip
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if p := strings.TrimSpace(parts[0]); p != "" {
			return p
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// Allow returns true if the request may proceed. On Redis errors, fails open.
func (rl *RateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	if rl == nil || rl.rdb == nil {
		return true, nil
	}
	tokensKey := "request_rate_limiter." + key + ".tokens"
	tsKey := "request_rate_limiter." + key + ".timestamp"
	now := time.Now().Unix()
	res, err := rl.script.Run(ctx, rl.rdb, []string{tokensKey, tsKey},
		rl.rate, rl.capacity, now, 1,
	).Int()
	if err != nil {
		return true, fmt.Errorf("redis rate limit: %w", err)
	}
	return res == 1, nil
}

func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/actuator/health" || strings.HasPrefix(r.URL.Path, "/actuator/") {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		ok, err := rl.Allow(r.Context(), rateLimitKey(r))
		if err != nil {
			log.Printf("rate limit redis error (fail-open): %v", err)
		}
		if !ok {
			applyCORS(w.Header(), r)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"title":"Too Many Requests","detail":"rate limit exceeded"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}
