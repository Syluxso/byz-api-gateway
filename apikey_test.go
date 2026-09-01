package main

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestParseApiKeyClaims(t *testing.T) {
	// {"grant_type":"user_api_key","token_id":"tid","app_id":"app1","organization_id":"oid","user_id":"uid"}
	payload := base64.RawURLEncoding.EncodeToString([]byte(
		`{"grant_type":"user_api_key","token_id":"tid","app_id":"app1","organization_id":"oid","user_id":"uid"}`,
	))
	token := "hdr." + payload + ".sig"
	c := parseApiKeyClaims("Bearer " + token)
	if c == nil {
		t.Fatal("expected claims")
	}
	if c.AppID != "app1" || c.TokenID != "tid" || c.UserID != "uid" {
		t.Fatalf("unexpected %#v", c)
	}

	if parseApiKeyClaims("Bearer byz_sk_secret") != nil {
		t.Fatal("raw secret should be ignored")
	}

	pwPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"grant_type":"password"}`))
	if parseApiKeyClaims("Bearer x."+pwPayload+".y") != nil {
		t.Fatal("password grant should be ignored")
	}
}

func TestMatchRoute(t *testing.T) {
	routes := loadRoutes()
	r, rest, ok := matchRoute(routes, "/iam/api/v1/login")
	if !ok || r.ID != "iam" || rest != "/api/v1/login" {
		t.Fatalf("got ok=%v id=%s rest=%s", ok, r.ID, rest)
	}
	r, rest, ok = matchRoute(routes, "/managed/actuator/health")
	if !ok || r.ID != "managed" || rest != "/actuator/health" {
		t.Fatalf("managed route: ok=%v id=%s rest=%s", ok, r.ID, rest)
	}
	r, rest, ok = matchRoute(routes, "/kan/api/v1/boards")
	if !ok || r.ID != "kan" || rest != "/api/v1/boards" {
		t.Fatalf("kan route: ok=%v id=%s rest=%s", ok, r.ID, rest)
	}
	r, rest, ok = matchRoute(routes, "/kan/healthz")
	if !ok || r.ID != "kan" || rest != "/healthz" {
		t.Fatalf("kan health: ok=%v id=%s rest=%s", ok, r.ID, rest)
	}
	r, rest, ok = matchRoute(routes, "/bb/healthz")
	if !ok || r.ID != "bb" || rest != "/healthz" {
		t.Fatalf("bb health: ok=%v id=%s rest=%s", ok, r.ID, rest)
	}
	r, rest, ok = matchRoute(routes, "/bb/api/me")
	if !ok || r.ID != "bb" || rest != "/api/me" {
		t.Fatalf("bb api: ok=%v id=%s rest=%s", ok, r.ID, rest)
	}
	_, _, ok = matchRoute(routes, "/nope")
	if ok {
		t.Fatal("expected no match")
	}
}

func TestAsStringJSONNumber(t *testing.T) {
	var claims map[string]any
	_ = json.Unmarshal([]byte(`{"x":1}`), &claims)
	// numbers become float64 — asString should still work via marshal fallback
	if asString(claims["x"]) == "" {
		t.Fatal("expected non-empty")
	}
}
