package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/Syluxso/byz-platform/events"
)

// Event types on the two gateway topics. Unchanged from the previous producer.
const (
	typeAccess = "gateway.request.completed"
	typeUsage  = "api.request"
)

// accessPayload is the domain part of byz.gateway.access. The platform
// envelope adds eventId, type, occurredAt, organizationId, tenantId, userId,
// producer and traceId. Consumers: server-health-emitter (raw JSON ring for
// the admin SSE feed). Field names are frozen.
type accessPayload struct {
	RequestID  string  `json:"requestId"`
	Method     string  `json:"method"`
	Path       string  `json:"path"`
	Status     int     `json:"status"`
	DurationMs int64   `json:"durationMs"`
	ClientIP   string  `json:"clientIp"`
	RouteID    *string `json:"routeId"`
	ClientID   *string `json:"clientId"`
}

// usagePayload is the domain part of byz.api.usage, emitted only for API-key
// tokens (user_api_key / tenant_api_key grants). Field names are frozen.
type usagePayload struct {
	TokenID    string `json:"tokenId"`
	AppID      string `json:"appId"`
	GrantType  string `json:"grantType"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	Status     int    `json:"status"`
	DurationMs int64  `json:"durationMs"`
}

// emitAfter publishes the access event for every non-actuator request and,
// for API-key tokens, the usage event. The bearer is only peeked (not
// verified) for organisation and client ids, exactly as before: the gateway
// does not validate proxied tokens, the backends do.
//
// Keys are unchanged: access by requestId, usage by tokenId.
func (g *gateway) emitAfter(ctx context.Context, r *http.Request, requestID, routeID, path string, status int, durationMs int64) {
	if !g.pub.Enabled() || strings.HasPrefix(path, "/actuator") {
		return
	}
	auth := r.Header.Get("Authorization")

	var orgID, clientID string
	if access := parseAccessClaims(auth); access != nil {
		orgID = access.OrganizationID
		clientID = access.ClientID
	}
	g.pub.EmitAsync(ctx, events.Event{
		Topic:          g.accessTopic,
		Key:            requestID,
		Type:           typeAccess,
		OrganizationID: orgID,
		Payload: accessPayload{
			RequestID:  requestID,
			Method:     r.Method,
			Path:       path,
			Status:     status,
			DurationMs: durationMs,
			ClientIP:   clientIP(r),
			RouteID:    strPtr(routeID),
			ClientID:   strPtr(clientID),
		},
	})

	claims := parseApiKeyClaims(auth)
	if claims == nil {
		return
	}
	g.pub.EmitAsync(ctx, events.Event{
		Topic:          g.usageTopic,
		Key:            claims.TokenID,
		Type:           typeUsage,
		OrganizationID: claims.OrganizationID,
		TenantID:       claims.TenantID,
		UserID:         claims.UserID,
		Payload: usagePayload{
			TokenID:    claims.TokenID,
			AppID:      claims.AppID,
			GrantType:  claims.GrantType,
			Method:     r.Method,
			Path:       path,
			Status:     status,
			DurationMs: durationMs,
		},
	})
}
