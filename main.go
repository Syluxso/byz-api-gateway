package main

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Syluxso/byz-platform/config"
	"github.com/Syluxso/byz-platform/events"
	"github.com/Syluxso/byz-platform/fail"
	"github.com/Syluxso/byz-platform/httpx"
	"github.com/Syluxso/byz-platform/iam"
	"github.com/Syluxso/byz-platform/iamclient"
	"github.com/Syluxso/byz-platform/obs"
	"github.com/Syluxso/byz-platform/shutdown"
)

const serviceName = "byz-api-gateway"

// gateway holds the edge's dependencies. main builds it from the environment;
// tests build it with a fixture verifier, an in-process Kafka and no Redis.
type gateway struct {
	routes      []Route
	limiter     *RateLimiter
	pub         *events.Publisher
	accessTopic string
	usageTopic  string
	transport   http.RoundTripper
	verifier    *iam.Verifier
	logBuf      *obs.Buffer
	rdb         *redis.Client
	payHooks    *payWebhooks // nil: Stripe webhooks answer 503
}

func main() {
	logger, logBuf := obs.New(serviceName, obs.Options{})
	cfg := config.Load(serviceName, config.Service{Port: "8096"})
	redisAddr := config.Env("REDIS_HOST", "127.0.0.1") + ":" + config.Env("REDIS_PORT", "6379")
	redisPass := os.Getenv("REDIS_PASSWORD")
	replenish := config.Int("RATE_LIMIT_REPLENISH", 40)
	burst := config.Int("RATE_LIMIT_BURST", 80)
	accessTopic := config.Env("KAFKA_TOPIC_GATEWAY_ACCESS", "byz.gateway.access")
	usageTopic := config.Env("KAFKA_TOPIC_API_USAGE", "byz.api.usage")
	ctx0 := context.Background()

	routes := loadRoutes()
	if len(routes) == 0 {
		log.Fatal("no backend routes configured")
	}

	verifier, err := iam.NewVerifier(ctx0, iam.Options{
		JWKSURL:  cfg.JWKSURL,
		Issuers:  cfg.Issuers,
		Audience: cfg.Audience,
		Realm:    serviceName,
		OnReject: obs.AuthRejected(serviceName),
	})
	if err != nil {
		log.Fatalf("jwks: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPass,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})
	ctxPing, cancelPing := context.WithTimeout(ctx0, 2*time.Second)
	if err := rdb.Ping(ctxPing).Err(); err != nil {
		log.Printf("redis ping warning: %v (rate limit will fail-open until Redis is up)", err)
	}
	cancelPing()

	pub, err := events.New(events.Options{Brokers: cfg.KafkaBrokers, Enabled: cfg.KafkaEnabled, Producer: serviceName, Logger: logger})
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	faults := fail.New(fail.Options{Service: serviceName, Logger: logger, Publisher: pub})
	fail.SetDefault(faults)

	// The gateway's own service identity, used only to read byz-pay webhook
	// secrets. Unset turns Stripe webhooks off (503).
	gatewayTokens := iamclient.NewTokenSource(
		config.Env("BYZ_IAM_URI", "http://127.0.0.1:8082"),
		config.Env("GATEWAY_CLIENT_ID", ""),
		os.Getenv("GATEWAY_CLIENT_SECRET"),
		nil,
	)
	payHooks := newPayWebhooks(
		config.Env("BYZ_PAY_URI", "http://127.0.0.1:8230"),
		gatewayTokens,
		pub,
		config.Env("KAFKA_TOPIC_PAY_STRIPE_WEBHOOK", "byz.pay.stripe.webhook"),
	)
	if payHooks == nil {
		log.Printf("stripe webhooks off: GATEWAY_CLIENT_ID / GATEWAY_CLIENT_SECRET not set")
	}

	g := &gateway{
		routes:      routes,
		limiter:     NewRateLimiter(rdb, replenish, burst),
		pub:         pub,
		accessTopic: accessTopic,
		usageTopic:  usageTopic,
		transport:   newTransport(),
		verifier:    verifier,
		logBuf:      logBuf,
		rdb:         rdb,
		payHooks:    payHooks,
	}

	// No RequestLogger here on purpose: the access event already records
	// every request, and a second line per request at the edge is noise.
	handler := httpx.Chain(g.handler(),
		httpx.RequestID,
		obs.HTTPMetrics(serviceName),
	)
	srv := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// No overall WriteTimeout: SSE and long responses must not be killed.
	}

	log.Printf("%s listening on http://%s redis=%s kafka=%v routes=%d", serviceName, cfg.Addr(), redisAddr, pub.Enabled(), len(routes))
	for _, rt := range routes {
		log.Printf("  route %s %s/** → %s", rt.ID, rt.Prefix, rt.Target.String())
	}
	if err := shutdown.Run(ctx0, srv, faults.Close, pub.Close, func(context.Context) error { return rdb.Close() }); err != nil {
		log.Fatalf("shutdown: %v", err)
	}
}

func newTransport() *http.Transport {
	return &http.Transport{
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
}

// handler builds the mux: local endpoints first, then everything else through
// the rate limiter into the reverse proxy.
func (g *gateway) handler() http.Handler {
	mux := http.NewServeMux()

	// Liveness bodies are written raw so they stay byte-identical to what the
	// admin health probe and nginx have seen since the cutover.
	up := func(w http.ResponseWriter, r *http.Request) {
		applyCORS(w.Header(), r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	}
	mux.HandleFunc("/actuator/health", up)
	mux.HandleFunc("/healthz", up)
	// Readiness is new: it reports Redis, which the liveness endpoints never
	// did, so the admin health row can see a rate-limit backend outage.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		applyCORS(w.Header(), r)
		if g.rdb != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := g.rdb.Ping(ctx).Err(); err != nil {
				httpx.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "DOWN", "detail": "redis: " + err.Error()})
				return
			}
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	})
	mux.Handle("GET /metrics", obs.Metrics())

	logs := obs.LogsHandler(g.logBuf)
	mux.Handle("/api/v1/admin/logs", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		applyCORS(w.Header(), r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		g.verifier.FromRequest(logs).ServeHTTP(w, r)
	}))

	// Stripe webhooks for byz-pay are verified and published here, never
	// proxied, and are not rate limited per IP (Stripe delivers in bursts).
	mux.Handle("POST /pay/webhooks/stripe/{configId}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		g.payHooks.ServeHTTP(sw, r)
		g.emitAfter(r.Context(), r, httpx.RequestIDFrom(r.Context()), "pay-webhook", r.URL.Path, sw.status, time.Since(started).Milliseconds())
	}))
	// byz-pay's internal routes (webhook secrets) are never reachable from outside.
	mux.HandleFunc("/pay/internal/", func(w http.ResponseWriter, r *http.Request) {
		applyCORS(w.Header(), r)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`404 page not found` + "\n"))
	})

	// Everything else: rate limit, then proxy.
	mux.Handle("/", g.limiter.Middleware(http.HandlerFunc(g.proxy)))
	return mux
}

// proxy is the reverse proxy path. Request ids come from httpx.RequestID (set
// on the context and already echoed on the response), so upstream echoes of
// X-Request-Id are dropped rather than duplicated.
func (g *gateway) proxy(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		applyCORS(w.Header(), r)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	path := r.URL.Path
	requestID := httpx.RequestIDFrom(r.Context())
	r.Header.Set(httpx.RequestIDHeader, requestID)
	r.Header.Del("Expect")

	route, rest, ok := matchRoute(g.routes, path)
	if !ok {
		applyCORS(w.Header(), r)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("404 page not found\n"))
		g.emitAfter(r.Context(), r, requestID, "", path, http.StatusNotFound, 0)
		return
	}
	// Label metrics by route id rather than the catch-all "/" pattern.
	r.Pattern = "proxy/" + route.ID

	started := time.Now()
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

	rp := httputil.NewSingleHostReverseProxy(route.Target)
	rp.Transport = g.transport
	rp.FlushInterval = -1
	rp.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		log.Printf("proxy error route=%s: %v", route.ID, err)
		fail.Report(req.Context(), fail.Fault{
			Code:  "gateway.upstream_unavailable",
			Err:   err,
			Msg:   "upstream unavailable",
			Attrs: map[string]any{"route": route.ID, "target": route.Target.Host, "path": path},
		})
		applyCORS(rw.Header(), req)
		httpx.WriteProblem(rw, http.StatusBadGateway, "gateway.upstream_unavailable", "upstream unavailable")
	}
	rp.Director = func(req *http.Request) {
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
		req.Header.Set(httpx.RequestIDHeader, requestID)
		req.Header.Del("Expect")
		if _, ok := req.Header["User-Agent"]; !ok {
			req.Header.Set("User-Agent", "")
		}
	}
	rp.ModifyResponse = func(resp *http.Response) error {
		stripCORS(resp.Header)
		applyCORS(resp.Header, r)
		// The response writer already carries X-Request-Id from the
		// middleware; an upstream echo would otherwise be added twice.
		resp.Header.Del(httpx.RequestIDHeader)
		return nil
	}

	rp.ServeHTTP(sw, r)

	g.emitAfter(r.Context(), r, requestID, route.ID, path, sw.status, time.Since(started).Milliseconds())
}

// statusWriter captures the upstream status for the access event while
// staying transparent to streaming and hijacking.
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

func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("hijack not supported")
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

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
