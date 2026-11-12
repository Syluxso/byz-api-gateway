package main

import (
	"net/url"
	"strings"

	"github.com/Syluxso/byz-platform/config"
)

type Route struct {
	ID     string
	Prefix string // e.g. "/iam"
	Target *url.URL
}

// loadRoutes reads the BYZ_*_URI route table. This stays in the gateway on
// purpose: routing policy is this service's domain, not the platform's.
func loadRoutes() []Route {
	defs := []struct {
		id, prefix, env, def string
	}{
		{"iam", "/iam", "BYZ_IAM_URI", "http://127.0.0.1:8082"},
		{"notifications", "/notifications", "BYZ_NOTIFICATIONS_URI", "http://127.0.0.1:8081"},
		{"directory", "/directory", "BYZ_DIRECTORY_URI", "http://127.0.0.1:8086"},
		{"events", "/events", "BYZ_EVENTS_URI", "http://127.0.0.1:8088"},
		{"files", "/files", "BYZ_FILES_URI", "http://127.0.0.1:8089"},
		{"search", "/search", "BYZ_SEARCH_URI", "http://127.0.0.1:8099"},
		{"ingest", "/ingest", "BYZ_INGEST_URI", "http://127.0.0.1:8100"},
		{"chat", "/chat", "BYZ_CHAT_URI", "http://127.0.0.1:8102"},
		{"agent", "/agent", "BYZ_AGENT_URI", "http://127.0.0.1:8103"},
		// Temporary alias while admin/deploy cut over from byz-compact.
		{"compact", "/compact", "BYZ_AGENT_URI", "http://127.0.0.1:8103"},
		{"fetch", "/fetch", "BYZ_FETCH_URI", "http://127.0.0.1:8104"},
		{"managed", "/managed", "BYZ_MANAGED_URI", "http://127.0.0.1:8105"},
		{"kan", "/kan", "BYZ_KAN_URI", "http://127.0.0.1:8109"},
		{"bb", "/bb", "BYZ_BB_URI", "http://127.0.0.1:8110"},
		// byz-pay. /pay/webhooks/stripe/* and /pay/internal/* never proxy:
		// the gateway handles the first itself and refuses the second.
		{"pay", "/pay", "BYZ_PAY_URI", "http://127.0.0.1:8230"},
	}
	out := make([]Route, 0, len(defs))
	for _, d := range defs {
		raw := config.Env(d.env, d.def)
		u, err := url.Parse(raw)
		if err != nil || u.Scheme == "" || u.Host == "" {
			continue
		}
		out = append(out, Route{ID: d.id, Prefix: d.prefix, Target: u})
	}
	return out
}

func matchRoute(routes []Route, path string) (Route, string, bool) {
	for _, r := range routes {
		if path == r.Prefix || strings.HasPrefix(path, r.Prefix+"/") {
			rest := strings.TrimPrefix(path, r.Prefix)
			if rest == "" {
				rest = "/"
			}
			return r, rest, true
		}
	}
	return Route{}, "", false
}
