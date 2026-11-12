package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Syluxso/byz-platform/iamclient"
)

const payTopic = "byz.pay.stripe.webhook"

func stripeSign(body []byte, secret string, ts time.Time) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.", ts.Unix())
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", ts.Unix(), hex.EncodeToString(mac.Sum(nil)))
}

type payFake struct {
	secret  atomic.Value // string
	fetches atomic.Int32
	srv     *httptest.Server
	iam     *httptest.Server
}

// withPay wires fake IAM (token endpoint) and fake byz-pay (internal secret
// route) into the fixture's gateway.
func withPay(t *testing.T, f *fixture) *payFake {
	t.Helper()
	p := &payFake{}
	p.secret.Store("whsec_one")
	p.iam = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/oauth/token" {
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write([]byte(`{"accessToken":"gw-token","expiresIn":3600}`))
	}))
	t.Cleanup(p.iam.Close)
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer gw-token" {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path != "/internal/v1/provider-configs/cfg-1/webhook-secret" {
			w.WriteHeader(404)
			return
		}
		p.fetches.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"configId": "cfg-1", "organizationId": "org-a", "tenantId": "t-1", "provider": "stripe",
			"mode": "live", "webhookSecret": p.secret.Load().(string),
		})
	}))
	t.Cleanup(p.srv.Close)
	tokens := iamclient.NewTokenSource(p.iam.URL, "byz-api-gateway", "s3cret", nil)
	f.g.payHooks = newPayWebhooks(p.srv.URL, tokens, f.g.pub, payTopic)
	return p
}

func (f *fixture) postWebhook(path string, body []byte, sig string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", path, strings.NewReader(string(body)))
	if sig != "" {
		req.Header.Set("Stripe-Signature", sig)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

var stripeEvent = []byte(`{"id":"evt_1","object":"event","type":"payment_intent.succeeded","livemode":true,"data":{"object":{"id":"pi_1"}}}`)

func TestStripeWebhookVerifiedAndPublished(t *testing.T) {
	f := newFixture(t)
	withPay(t, f)
	rec := f.postWebhook("/pay/webhooks/stripe/cfg-1", stripeEvent, stripeSign(stripeEvent, "whsec_one", time.Now()))
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if len(f.seen) != 0 {
		t.Fatal("webhook was proxied upstream")
	}
	recs := f.records(t, payTopic)
	if len(recs) != 1 {
		t.Fatalf("%d records", len(recs))
	}
	r := recs[0]
	if r["type"] != "stripe.webhook.received" || r["configId"] != "cfg-1" || r["organizationId"] != "org-a" ||
		r["tenantId"] != "t-1" || r["stripeEventId"] != "evt_1" || r["body"] != string(stripeEvent) || r["_key"] != "cfg-1" {
		t.Fatalf("record: %v", r)
	}
	if !strings.HasPrefix(r["signature"].(string), "t=") {
		t.Fatalf("signature not forwarded: %v", r["signature"])
	}
}

func TestStripeWebhookRejections(t *testing.T) {
	f := newFixture(t)
	withPay(t, f)
	now := time.Now()
	cases := []struct {
		name, path, sig string
		body            []byte
		want            int
	}{
		{"wrong secret", "/pay/webhooks/stripe/cfg-1", stripeSign(stripeEvent, "whsec_other", now), stripeEvent, 400},
		{"tampered body", "/pay/webhooks/stripe/cfg-1", stripeSign(stripeEvent, "whsec_one", now), append([]byte(" "), stripeEvent...), 400},
		{"replayed old delivery", "/pay/webhooks/stripe/cfg-1", stripeSign(stripeEvent, "whsec_one", now.Add(-10*time.Minute)), stripeEvent, 400},
		{"no signature", "/pay/webhooks/stripe/cfg-1", "", stripeEvent, 400},
		{"unknown config", "/pay/webhooks/stripe/cfg-9", stripeSign(stripeEvent, "whsec_one", now), stripeEvent, 404},
		{"not an event", "/pay/webhooks/stripe/cfg-1", stripeSign([]byte(`{}`), "whsec_one", now), []byte(`{}`), 400},
	}
	for _, c := range cases {
		if rec := f.postWebhook(c.path, c.body, c.sig); rec.Code != c.want {
			t.Errorf("%s: status %d, want %d: %s", c.name, rec.Code, c.want, rec.Body)
		}
	}
	if n := len(f.records(t, payTopic)); n != 0 {
		t.Fatalf("%d records published for rejected deliveries", n)
	}
}

func TestStripeWebhookSecretRotation(t *testing.T) {
	f := newFixture(t)
	p := withPay(t, f)
	clock := time.Now()
	f.g.payHooks.now = func() time.Time { return clock }

	if rec := f.postWebhook("/pay/webhooks/stripe/cfg-1", stripeEvent, stripeSign(stripeEvent, "whsec_one", clock)); rec.Code != 200 {
		t.Fatalf("first: %d %s", rec.Code, rec.Body)
	}
	// Cached: a second delivery does not refetch.
	f.postWebhook("/pay/webhooks/stripe/cfg-1", stripeEvent, stripeSign(stripeEvent, "whsec_one", clock))
	if p.fetches.Load() != 1 {
		t.Fatalf("fetches = %d, want 1 (cached)", p.fetches.Load())
	}
	// The operator rotates the secret in byz-pay; a minute later Stripe signs with the new one.
	p.secret.Store("whsec_two")
	clock = clock.Add(time.Minute)
	rec := f.postWebhook("/pay/webhooks/stripe/cfg-1", stripeEvent, stripeSign(stripeEvent, "whsec_two", clock))
	if rec.Code != 200 || p.fetches.Load() != 2 {
		t.Fatalf("after rotation: %d (fetches %d) %s", rec.Code, p.fetches.Load(), rec.Body)
	}
}

func TestPayInternalRoutesNeverProxied(t *testing.T) {
	f := newFixture(t)
	req := httptest.NewRequest("GET", "/pay/internal/v1/provider-configs/cfg-1/webhook-secret", nil)
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	if rec.Code != 404 || len(f.seen) != 0 {
		t.Fatalf("internal route: %d, upstream hits %d", rec.Code, len(f.seen))
	}
}

func TestStripeWebhookOffWithoutCredentials(t *testing.T) {
	f := newFixture(t)
	f.g.payHooks = newPayWebhooks("http://127.0.0.1:1", iamclient.NewTokenSource("", "", "", nil), f.g.pub, payTopic)
	if f.g.payHooks != nil {
		t.Fatal("expected nil without credentials")
	}
	if rec := f.postWebhook("/pay/webhooks/stripe/cfg-1", stripeEvent, "t=1,v1=00"); rec.Code != 503 {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestVerifyStripeSignatureAcceptsAnyV1(t *testing.T) {
	now := time.Now()
	good := stripeSign(stripeEvent, "whsec_one", now)
	ts, v1, _ := strings.Cut(good, ",")
	header := ts + ",v1=deadbeef," + v1 + ",v0=ignored"
	if err := verifyStripeSignature(stripeEvent, header, "whsec_one", now, stripeTolerance); err != nil {
		t.Fatal(err)
	}
	if err := verifyStripeSignature(stripeEvent, "garbage", "whsec_one", now, stripeTolerance); err == nil {
		t.Fatal("garbage header verified")
	}
}
