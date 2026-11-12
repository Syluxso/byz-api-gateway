package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Syluxso/byz-platform/events"
	"github.com/Syluxso/byz-platform/httpx"
	"github.com/Syluxso/byz-platform/iam"
	"github.com/Syluxso/byz-platform/obs"
)

type fixture struct {
	key      *rsa.PrivateKey
	g        *gateway
	handler  http.Handler
	cluster  *kfake.Cluster
	upstream *httptest.Server
	seen     []*http.Request
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f.key = key
	v, err := iam.NewVerifier(context.Background(), iam.Options{
		Keyfunc: func(*jwt.Token) (any, error) { return &key.PublicKey, nil },
		Realm:   serviceName,
	})
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "byz.gateway.access", "byz.api.usage", "byz.pay.stripe.webhook"))
	if err != nil {
		t.Skipf("kfake unavailable: %v", err)
	}
	t.Cleanup(cluster.Close)
	f.cluster = cluster
	pub, err := events.New(events.Options{Brokers: cluster.ListenAddrs(), Enabled: true, Producer: serviceName, ProduceTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	// Upstream echoes an X-Request-Id and its own CORS header so the gateway's
	// dedupe and strip behaviour is exercised.
	f.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.seen = append(f.seen, r.Clone(context.Background()))
		w.Header().Set("X-Request-Id", r.Header.Get("X-Request-Id"))
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"ok":true,"path":"` + r.URL.Path + `"}`))
	}))
	t.Cleanup(f.upstream.Close)
	target, _ := url.Parse(f.upstream.URL)
	dead, _ := url.Parse("http://127.0.0.1:1")

	f.g = &gateway{
		routes:      []Route{{ID: "kan", Prefix: "/kan", Target: target}, {ID: "dead", Prefix: "/dead", Target: dead}},
		limiter:     nil, // nil limiter allows everything (no Redis in tests)
		pub:         pub,
		accessTopic: "byz.gateway.access",
		usageTopic:  "byz.api.usage",
		transport:   newTransport(),
		verifier:    v,
		logBuf:      obs.NewBuffer(0),
	}
	f.handler = httpx.Chain(f.g.handler(), httpx.RequestID, obs.HTTPMetrics(serviceName))
	return f
}

func (f *fixture) signed(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	claims["exp"] = time.Now().Add(time.Hour).Unix()
	s, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(f.key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// unsigned builds a token the gateway will only peek at (it never verifies
// proxied bearers), mirroring a real IAM token's payload.
func unsigned(claims map[string]any) string {
	b, _ := json.Marshal(claims)
	return "hdr." + base64.RawURLEncoding.EncodeToString(b) + ".sig"
}

func (f *fixture) records(t *testing.T, topic string) []map[string]any {
	t.Helper()
	if err := f.g.pub.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	c, err := kgo.NewClient(kgo.SeedBrokers(f.cluster.ListenAddrs()...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out []map[string]any
	fetches := c.PollFetches(ctx)
	fetches.EachRecord(func(r *kgo.Record) {
		var m map[string]any
		_ = json.Unmarshal(r.Value, &m)
		m["_key"] = string(r.Key)
		out = append(out, m)
	})
	return out
}

func TestProxyRequestIDAndCORS(t *testing.T) {
	f := newFixture(t)
	req := httptest.NewRequest("GET", "/kan/api/v1/boards?x=1", nil)
	req.Header.Set("Origin", "https://sys.byzantineapp.dev")
	req.Header.Set("X-Request-Id", "rid-1")
	req.Header.Set("Authorization", "Bearer "+unsigned(map[string]any{"organization_id": "org-1", "client_id": "byz-admin", "grant_type": "password", "sub": "u1"}))
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, req)

	if rr.Code != 201 || !strings.Contains(rr.Body.String(), `"path":"/api/v1/boards"`) {
		t.Fatalf("proxy: %d %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Values("X-Request-Id"); len(got) != 1 || got[0] != "rid-1" {
		t.Fatalf("X-Request-Id must appear exactly once with the caller's id, got %v", got)
	}
	if got := rr.Header().Values("Access-Control-Allow-Origin"); len(got) != 1 || got[0] != "https://sys.byzantineapp.dev" {
		t.Fatalf("upstream CORS must be replaced by the gateway's, got %v", got)
	}
	if len(f.seen) != 1 || f.seen[0].Header.Get("X-Request-Id") != "rid-1" || f.seen[0].URL.RawQuery != "x=1" {
		t.Fatalf("upstream request: %+v", f.seen)
	}

	recs := f.records(t, "byz.gateway.access")
	if len(recs) != 1 {
		t.Fatalf("access records = %d", len(recs))
	}
	ev := recs[0]
	want := map[string]any{
		"_key": "rid-1", "type": "gateway.request.completed", "requestId": "rid-1", "traceId": "rid-1",
		"method": "GET", "path": "/kan/api/v1/boards", "status": float64(201), "routeId": "kan",
		"organizationId": "org-1", "clientId": "byz-admin", "tenantId": nil, "userId": nil,
		"producer": serviceName,
	}
	for k, v := range want {
		if got, ok := ev[k]; !ok || got != v {
			t.Errorf("%s = %v (present=%v), want %v", k, got, ok, v)
		}
	}
	for _, k := range []string{"eventId", "occurredAt", "durationMs", "clientIp"} {
		if _, ok := ev[k]; !ok {
			t.Errorf("missing %s", k)
		}
	}
}

func TestUsageEventForAPIKeyToken(t *testing.T) {
	f := newFixture(t)
	req := httptest.NewRequest("POST", "/kan/api/v1/tickets", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer "+unsigned(map[string]any{
		"grant_type": "user_api_key", "token_id": "tok-9", "app_id": "app_1", "organization_id": "org-1", "user_id": "u-1", "tenant_id": "t-1",
	}))
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, req)
	if rr.Code != 201 {
		t.Fatalf("proxy: %d", rr.Code)
	}
	recs := f.records(t, "byz.api.usage")
	if len(recs) != 1 {
		t.Fatalf("usage records = %d", len(recs))
	}
	ev := recs[0]
	want := map[string]any{
		"_key": "tok-9", "type": "api.request", "tokenId": "tok-9", "appId": "app_1", "grantType": "user_api_key",
		"organizationId": "org-1", "userId": "u-1", "tenantId": "t-1", "method": "POST", "path": "/kan/api/v1/tickets", "status": float64(201),
	}
	for k, v := range want {
		if got := ev[k]; got != v {
			t.Errorf("%s = %v, want %v", k, got, v)
		}
	}
}

func TestNotFoundBytesAndEvent(t *testing.T) {
	f := newFixture(t)
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, httptest.NewRequest("GET", "/nope", nil))
	if rr.Code != 404 || rr.Body.String() != "404 page not found\n" {
		t.Fatalf("404: %d %q", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("X-Request-Id") == "" {
		t.Fatal("404 must carry a request id")
	}
	recs := f.records(t, "byz.gateway.access")
	if len(recs) != 1 || recs[0]["routeId"] != nil || recs[0]["status"] != float64(404) || recs[0]["organizationId"] != nil {
		t.Fatalf("404 event = %v", recs)
	}
}

func TestUpstreamDownIs502Problem(t *testing.T) {
	f := newFixture(t)
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, httptest.NewRequest("GET", "/dead/x", nil))
	if rr.Code != 502 || !strings.Contains(rr.Body.String(), "gateway.upstream_unavailable") {
		t.Fatalf("502: %d %s", rr.Code, rr.Body.String())
	}
	recs := f.records(t, "byz.gateway.access")
	if len(recs) != 1 || recs[0]["status"] != float64(502) || recs[0]["routeId"] != "dead" {
		t.Fatalf("502 event = %v", recs)
	}
}

func TestLocalEndpoints(t *testing.T) {
	f := newFixture(t)

	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, httptest.NewRequest("GET", "/actuator/health", nil))
	if rr.Code != 200 || rr.Body.String() != `{"status":"UP"}` {
		t.Fatalf("health bytes changed: %d %q", rr.Code, rr.Body.String())
	}
	rr = httptest.NewRecorder()
	f.handler.ServeHTTP(rr, httptest.NewRequest("GET", "/readyz", nil))
	if rr.Code != 200 {
		t.Fatalf("readyz without redis should be UP: %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	f.handler.ServeHTTP(rr, httptest.NewRequest("GET", "/api/v1/admin/logs", nil))
	if rr.Code != 401 || !strings.Contains(rr.Header().Get("WWW-Authenticate"), serviceName) {
		t.Fatalf("admin logs without token: %d %v", rr.Code, rr.Header())
	}
	req := httptest.NewRequest("GET", "/api/v1/admin/logs?lines=5", nil)
	req.Header.Set("Authorization", "Bearer "+f.signed(t, jwt.MapClaims{"sub": "u1", "organization_id": "o1"}))
	rr = httptest.NewRecorder()
	f.handler.ServeHTTP(rr, req)
	if rr.Code != 200 || !strings.HasPrefix(strings.TrimSpace(rr.Body.String()), "[") {
		t.Fatalf("admin logs: %d %s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest("OPTIONS", "/kan/x", nil)
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("Access-Control-Request-Headers", "authorization, x-custom")
	rr = httptest.NewRecorder()
	f.handler.ServeHTTP(rr, req)
	if rr.Code != 204 || rr.Header().Get("Access-Control-Allow-Headers") != "authorization, x-custom" || rr.Header().Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("preflight: %d %v", rr.Code, rr.Header())
	}

	// Health probes must not produce access events.
	if recs := f.records(t, "byz.gateway.access"); len(recs) != 0 {
		t.Fatalf("health/admin produced events: %v", recs)
	}
}
