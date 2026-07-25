package main

import (
	"net/http"
	"strings"
)

const allowHeadersDefault = "Authorization, Content-Type, Accept, Origin, X-Requested-With, X-Request-Id"

func applyCORS(h http.Header, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}
	stripCORS(h)
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Credentials", "true")
	h.Set("Vary", "Origin")
	h.Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS,HEAD")
	reqHeaders := r.Header.Get("Access-Control-Request-Headers")
	if strings.TrimSpace(reqHeaders) != "" {
		h.Set("Access-Control-Allow-Headers", reqHeaders)
	} else {
		h.Set("Access-Control-Allow-Headers", allowHeadersDefault)
	}
	h.Set("Access-Control-Expose-Headers", "X-Request-Id")
	h.Set("Access-Control-Max-Age", "3600")
}

func stripCORS(h http.Header) {
	h.Del("Access-Control-Allow-Origin")
	h.Del("Access-Control-Allow-Credentials")
	h.Del("Access-Control-Allow-Methods")
	h.Del("Access-Control-Allow-Headers")
	h.Del("Access-Control-Expose-Headers")
	h.Del("Access-Control-Max-Age")
}
