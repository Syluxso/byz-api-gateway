package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

type ApiKeyClaims struct {
	OrganizationID string
	TokenID        string
	AppID          string
	GrantType      string
	UserID         string
	TenantID       string
}

// AccessClaims are org/client fields peeked from any IAM JWT for access logging.
type AccessClaims struct {
	OrganizationID string
	ClientID       string
}

func parseAccessClaims(authorization string) *AccessClaims {
	claims := peekJWTClaims(authorization)
	if claims == nil {
		return nil
	}
	orgID := asString(claims["organization_id"])
	clientID := asString(claims["client_id"])
	if clientID == "" {
		clientID = asString(claims["app_id"])
	}
	if orgID == "" && clientID == "" {
		return nil
	}
	return &AccessClaims{OrganizationID: orgID, ClientID: clientID}
}

func peekJWTClaims(authorization string) map[string]any {
	if strings.TrimSpace(authorization) == "" {
		return nil
	}
	token := authorization
	if strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
		token = strings.TrimSpace(authorization[7:])
	}
	if token == "" || strings.HasPrefix(token, "byz_sk_") {
		return nil
	}
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		payload, err = base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil
		}
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil
	}
	return claims
}

func parseApiKeyClaims(authorization string) *ApiKeyClaims {
	claims := peekJWTClaims(authorization)
	if claims == nil {
		return nil
	}
	grant := asString(claims["grant_type"])
	if grant != "user_api_key" && grant != "tenant_api_key" {
		return nil
	}
	tokenID := asString(claims["token_id"])
	appID := asString(claims["app_id"])
	orgID := asString(claims["organization_id"])
	if tokenID == "" || appID == "" || orgID == "" {
		return nil
	}
	return &ApiKeyClaims{
		OrganizationID: orgID,
		TokenID:        tokenID,
		AppID:          appID,
		GrantType:      grant,
		UserID:         asString(claims["user_id"]),
		TenantID:       asString(claims["tenant_id"]),
	}
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return strings.TrimSpace(strings.Trim(strings.ReplaceAll(toString(v), "\"", ""), " "))
	}
	s = strings.TrimSpace(s)
	if s == "" || s == "null" {
		return ""
	}
	return s
}

func toString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
