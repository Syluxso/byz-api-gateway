package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

func withJWT(k keyfunc.Keyfunc, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			applyCORS(w.Header(), r)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			applyCORS(w.Header(), r)
			writeProblem(w, http.StatusUnauthorized, "Unauthorized", "missing bearer token")
			return
		}
		tokenStr := strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
		tok, err := jwt.Parse(tokenStr, k.Keyfunc,
			jwt.WithValidMethods([]string{"RS256"}),
			jwt.WithExpirationRequired(),
		)
		if err != nil || !tok.Valid {
			applyCORS(w.Header(), r)
			writeProblem(w, http.StatusUnauthorized, "Unauthorized", "invalid token")
			return
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, status int, title, detail string) {
	writeJSON(w, status, map[string]any{
		"title":  title,
		"detail": detail,
		"status": status,
	})
}
