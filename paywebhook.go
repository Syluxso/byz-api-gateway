package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Syluxso/byz-platform/events"
	"github.com/Syluxso/byz-platform/fail"
	"github.com/Syluxso/byz-platform/httpx"
	"github.com/Syluxso/byz-platform/iamclient"
)

// Stripe webhooks for byz-pay.
//
//	POST /pay/webhooks/stripe/{configId}
//
// Stripe posts here (the URL is set per provider config in the Stripe
// dashboard). The gateway:
//
//  1. reads that config's signing secret from byz-pay's internal route with
//     its own service token (cached; refetched once on a signature miss, so
//     a rotated secret takes effect without a restart),
//  2. checks Stripe-Signature over the exact raw body within a 5 minute
//     window (replays of old deliveries are refused),
//  3. publishes the raw body and signature to byz.pay.stripe.webhook and
//     waits for Kafka to accept it.
//
// Only then does it answer 200. Any failure answers non-2xx so Stripe
// retries. byz-pay re-checks the signature when it applies the record.
const (
	stripeWebhookType    = "stripe.webhook.received"
	stripeTolerance      = 5 * time.Minute
	maxStripeWebhookBody = 512 << 10
	secretTTL            = 5 * time.Minute
	secretRefetchAfter   = 30 * time.Second
)

var errUnknownConfig = errors.New("unknown provider config")

type paySecret struct {
	ConfigID       string `json:"configId"`
	OrganizationID string `json:"organizationId"`
	TenantID       string `json:"tenantId"`
	Provider       string `json:"provider"`
	Mode           string `json:"mode"`
	WebhookSecret  string `json:"webhookSecret"`
}

type cachedSecret struct {
	secret  paySecret
	missing bool
	at      time.Time
}

// payWebhooks verifies and forwards Stripe webhooks. A nil *payWebhooks
// answers 503: the gateway has no service credentials for byz-pay.
type payWebhooks struct {
	payBase string
	tokens  *iamclient.TokenSource
	client  *http.Client
	pub     *events.Publisher
	topic   string
	now     func() time.Time

	mu    sync.Mutex
	cache map[string]cachedSecret
}

func newPayWebhooks(payBase string, tokens *iamclient.TokenSource, pub *events.Publisher, topic string) *payWebhooks {
	if payBase == "" || tokens == nil || !tokens.Configured() {
		return nil
	}
	return &payWebhooks{
		payBase: strings.TrimRight(payBase, "/"),
		tokens:  tokens,
		client:  &http.Client{Timeout: 10 * time.Second},
		pub:     pub,
		topic:   topic,
		now:     time.Now,
		cache:   map[string]cachedSecret{},
	}
}

func (p *payWebhooks) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p == nil {
		httpx.WriteProblem(w, http.StatusServiceUnavailable, "pay.webhooks_disabled", "payment webhooks are not configured on this gateway")
		return
	}
	if !p.pub.Enabled() {
		// Without Kafka the event would be dropped; make Stripe retry instead.
		httpx.WriteProblem(w, http.StatusServiceUnavailable, "pay.kafka_disabled", "event bus unavailable")
		return
	}
	configID := r.PathValue("configId")
	body, err := io.ReadAll(io.LimitReader(r.Body, maxStripeWebhookBody+1))
	if err != nil {
		httpx.WriteProblem(w, http.StatusBadRequest, "pay.webhook_body", "could not read body")
		return
	}
	if len(body) > maxStripeWebhookBody {
		httpx.WriteProblem(w, http.StatusRequestEntityTooLarge, "pay.webhook_body", "body too large")
		return
	}
	sig := r.Header.Get("Stripe-Signature")
	if sig == "" || configID == "" {
		httpx.WriteProblem(w, http.StatusBadRequest, "pay.webhook_signature", "missing Stripe-Signature")
		return
	}

	ctx := r.Context()
	sec, fetchedAt, err := p.secret(ctx, configID, false)
	if errors.Is(err, errUnknownConfig) {
		httpx.WriteProblem(w, http.StatusNotFound, "pay.webhook_config", "unknown webhook endpoint")
		return
	}
	if err != nil {
		fail.Report(ctx, fail.Fault{Code: "gateway.pay_secret_unavailable", Err: err, Msg: "could not read webhook secret from byz-pay",
			Attrs: map[string]any{"configId": configID}})
		httpx.WriteProblem(w, http.StatusServiceUnavailable, "pay.unavailable", "payment service unavailable")
		return
	}
	verr := verifyStripeSignature(body, sig, sec.WebhookSecret, p.now(), stripeTolerance)
	if verr != nil && p.now().Sub(fetchedAt) > secretRefetchAfter {
		// The secret may have been rotated since it was cached.
		if fresh, _, ferr := p.secret(ctx, configID, true); ferr == nil {
			sec = fresh
			verr = verifyStripeSignature(body, sig, sec.WebhookSecret, p.now(), stripeTolerance)
		}
	}
	if verr != nil {
		httpx.WriteProblem(w, http.StatusBadRequest, "pay.webhook_signature", "signature did not verify")
		return
	}

	var head struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Livemode bool   `json:"livemode"`
	}
	if err := json.Unmarshal(body, &head); err != nil || head.ID == "" || head.Type == "" {
		httpx.WriteProblem(w, http.StatusBadRequest, "pay.webhook_body", "not a Stripe event")
		return
	}

	err = p.pub.Emit(ctx, events.Event{
		Topic:          p.topic,
		Key:            configID,
		Type:           stripeWebhookType,
		OrganizationID: sec.OrganizationID,
		TenantID:       sec.TenantID,
		Payload: map[string]any{
			"configId":        configID,
			"provider":        "stripe",
			"stripeEventId":   head.ID,
			"stripeEventType": head.Type,
			"livemode":        head.Livemode,
			"signature":       sig,
			// The exact bytes, as a string: byz-pay re-checks the signature.
			"body": string(body),
		},
	})
	if err != nil {
		fail.Report(ctx, fail.Fault{Code: "gateway.pay_webhook_publish", Err: err, Msg: "could not publish stripe webhook",
			Attrs: map[string]any{"configId": configID, "stripeEventId": head.ID}})
		httpx.WriteProblem(w, http.StatusServiceUnavailable, "pay.kafka_unavailable", "could not record event")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]bool{"received": true})
}

// secret returns the config's webhook secret and when it was fetched.
func (p *payWebhooks) secret(ctx context.Context, configID string, force bool) (paySecret, time.Time, error) {
	p.mu.Lock()
	c, ok := p.cache[configID]
	p.mu.Unlock()
	if ok && !force && p.now().Sub(c.at) < secretTTL {
		if c.missing {
			return paySecret{}, c.at, errUnknownConfig
		}
		return c.secret, c.at, nil
	}
	sec, err := p.fetch(ctx, configID)
	if err != nil && !errors.Is(err, errUnknownConfig) {
		return paySecret{}, time.Time{}, err
	}
	at := p.now()
	p.mu.Lock()
	p.cache[configID] = cachedSecret{secret: sec, missing: err != nil, at: at}
	p.mu.Unlock()
	return sec, at, err
}

func (p *payWebhooks) fetch(ctx context.Context, configID string) (paySecret, error) {
	tok, err := p.tokens.Token(ctx)
	if err != nil {
		return paySecret{}, fmt.Errorf("service token: %w", err)
	}
	u := p.payBase + "/internal/v1/provider-configs/" + pathEscape(configID) + "/webhook-secret"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return paySecret{}, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if id := httpx.RequestIDFrom(ctx); id != "" {
		req.Header.Set(httpx.RequestIDHeader, id)
	}
	res, err := p.client.Do(req)
	if err != nil {
		return paySecret{}, err
	}
	defer res.Body.Close()
	switch {
	case res.StatusCode == http.StatusNotFound:
		return paySecret{}, errUnknownConfig
	case res.StatusCode == http.StatusUnauthorized:
		p.tokens.Invalidate()
		return paySecret{}, fmt.Errorf("byz-pay rejected the gateway token")
	case res.StatusCode != http.StatusOK:
		return paySecret{}, fmt.Errorf("byz-pay answered %d", res.StatusCode)
	}
	var sec paySecret
	if err := json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&sec); err != nil {
		return paySecret{}, err
	}
	if sec.WebhookSecret == "" {
		return paySecret{}, errUnknownConfig
	}
	return sec, nil
}

func pathEscape(s string) string {
	// Config ids are UUIDs; anything else cannot be one.
	for _, c := range s {
		if !(c == '-' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			return "invalid"
		}
	}
	return s
}

// verifyStripeSignature implements Stripe's v1 scheme: Stripe-Signature is
// "t=<unix>,v1=<hex hmac>[,v1=...]" and the HMAC-SHA256 is over
// "<t>.<raw body>" with the endpoint's signing secret.
func verifyStripeSignature(body []byte, header, secret string, now time.Time, tolerance time.Duration) error {
	if secret == "" {
		return errors.New("no secret")
	}
	var ts int64
	var sigs [][]byte
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "t":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return errors.New("bad timestamp")
			}
			ts = n
		case "v1":
			if b, err := hex.DecodeString(v); err == nil {
				sigs = append(sigs, b)
			}
		}
	}
	if ts == 0 || len(sigs) == 0 {
		return errors.New("malformed header")
	}
	age := now.Sub(time.Unix(ts, 0))
	if age > tolerance || age < -tolerance {
		return errors.New("timestamp outside tolerance")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	want := mac.Sum(nil)
	for _, s := range sigs {
		if hmac.Equal(s, want) {
			return nil
		}
	}
	return errors.New("no matching signature")
}
