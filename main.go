package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func main() {
	port := env("PORT", "8096")
	bind := env("BIND", "0.0.0.0")
	redisAddr := env("REDIS_HOST", "127.0.0.1") + ":" + env("REDIS_PORT", "6379")
	redisPass := os.Getenv("REDIS_PASSWORD")
	replenish := envInt("RATE_LIMIT_REPLENISH", 40)
	burst := envInt("RATE_LIMIT_BURST", 80)
	kafkaEnabled := envBool("BYZ_KAFKA_ENABLED", true)
	kafkaBootstrap := env("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
	accessTopic := env("KAFKA_TOPIC_GATEWAY_ACCESS", "byz.gateway.access")
	usageTopic := env("KAFKA_TOPIC_API_USAGE", "byz.api.usage")

	routes := loadRoutes()
	if len(routes) == 0 {
		log.Fatal("no backend routes configured")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPass,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})
	ctxPing, cancelPing := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rdb.Ping(ctxPing).Err(); err != nil {
		log.Printf("redis ping warning: %v (rate limit will fail-open until Redis is up)", err)
	}
	cancelPing()

	limiter := NewRateLimiter(rdb, replenish, burst)

	publisher, err := NewKafkaPublisher(splitCSV(kafkaBootstrap), kafkaEnabled, accessTopic, usageTopic)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer publisher.Close()

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 0,
		ResponseHeaderTimeout: 30 * time.Second,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/actuator/health", func(w http.ResponseWriter, r *http.Request) {
		applyCORS(w.Header(), r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		applyCORS(w.Header(), r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})

	gateway := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			applyCORS(w.Header(), r)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		path := r.URL.Path
		route, rest, ok := matchRoute(routes, path)
		requestID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if requestID == "" {
			requestID = uuid.NewString()
		}
		r.Header.Set("X-Request-Id", requestID)
		r.Header.Del("Expect")

		if !ok {
			applyCORS(w.Header(), r)
			w.Header().Set("X-Request-Id", requestID)
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("404 page not found\n"))
			emitAfter(publisher, r, requestID, "", path, http.StatusNotFound, 0)
			return
		}

		started := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		sw.Header().Set("X-Request-Id", requestID)

		proxy := httputil.NewSingleHostReverseProxy(route.Target)
		proxy.Transport = transport
		proxy.FlushInterval = -1
		proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
			log.Printf("proxy error route=%s: %v", route.ID, err)
			applyCORS(rw.Header(), req)
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusBadGateway)
			_, _ = rw.Write([]byte(`{"title":"Bad Gateway","detail":"upstream unavailable"}`))
		}
		proxy.Director = func(req *http.Request) {
			targetQuery := route.Target.RawQuery
			req.URL.Scheme = route.Target.Scheme
			req.URL.Host = route.Target.Host
			req.URL.Path = singleJoiningSlash(route.Target.Path, rest)
			req.URL.RawPath = ""
			if targetQuery == "" || req.URL.RawQuery == "" {
				req.URL.RawQuery = targetQuery + req.URL.RawQuery
			} else {
				req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
			}
			req.Host = route.Target.Host
			req.Header.Set("X-Request-Id", requestID)
			req.Header.Del("Expect")
			if _, ok := req.Header["User-Agent"]; !ok {
				req.Header.Set("User-Agent", "")
			}
		}
		proxy.ModifyResponse = func(resp *http.Response) error {
			stripCORS(resp.Header)
			applyCORS(resp.Header, r)
			resp.Header.Set("X-Request-Id", requestID)
			return nil
		}

		proxy.ServeHTTP(sw, r)

		durationMs := time.Since(started).Milliseconds()
		emitAfter(publisher, r, requestID, route.ID, path, sw.status, durationMs)
	})

	// Health routes live on mux; everything else goes through rate limit + proxy.
	// (Previously mux was built but never set as Server.Handler.)
	mux.Handle("/", limiter.Middleware(gateway))

	addr := bind + ":" + port
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// No overall WriteTimeout — SSE / long responses must not be killed.
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("byz-api-gateway listening on http://%s redis=%s kafka=%v routes=%d",
			addr, redisAddr, kafkaEnabled, len(routes))
		for _, rt := range routes {
			log.Printf("  route %s %s/** → %s", rt.ID, rt.Prefix, rt.Target.String())
		}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	_ = rdb.Close()
	log.Printf("shutdown")
}

func emitAfter(p *KafkaPublisher, r *http.Request, requestID, routeID, path string, status int, durationMs int64) {
	if strings.HasPrefix(path, "/actuator") {
		return
	}
	occurred := time.Now().UTC().Format(time.RFC3339Nano)
	auth := r.Header.Get("Authorization")
	access := parseAccessClaims(auth)
	var orgID, clientID *string
	if access != nil {
		orgID = strPtr(access.OrganizationID)
		clientID = strPtr(access.ClientID)
	}
	p.PublishAccess(GatewayAccessEvent{
		EventID:        uuid.NewString(),
		Type:           "gateway.request.completed",
		OccurredAt:     occurred,
		RequestID:      requestID,
		Method:         r.Method,
		Path:           path,
		Status:         status,
		DurationMs:     durationMs,
		ClientIP:       clientIP(r),
		RouteID:        strPtr(routeID),
		OrganizationID: orgID,
		ClientID:       clientID,
	})

	claims := parseApiKeyClaims(auth)
	if claims == nil {
		return
	}
	p.PublishUsage(ApiUsageEvent{
		EventID:        uuid.NewString(),
		Type:           "api.request",
		OccurredAt:     occurred,
		OrganizationID: claims.OrganizationID,
		TokenID:        claims.TokenID,
		AppID:          claims.AppID,
		GrantType:      claims.GrantType,
		UserID:         strPtr(claims.UserID),
		TenantID:       strPtr(claims.TenantID),
		Method:         r.Method,
		Path:           path,
		Status:         status,
		DurationMs:     durationMs,
	})
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	default:
		return a + b
	}
}
