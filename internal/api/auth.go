package api

import (
	"context"
	"log/slog"
	"net/http"
)

// APIKeyValidator checks an API key without exposing its stored hash.
type APIKeyValidator interface {
	ValidateAPIKey(context.Context, string) (bool, error)
}

// APIKeyMiddleware protects a handler with the X-Api-Key header.
func APIKeyMiddleware(validator APIKeyValidator, log *slog.Logger, next http.Handler) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if validator == nil {
			writeError(w, http.StatusServiceUnavailable, codeAuthUnavailable, "API key authentication is not configured")
			return
		}
		valid, err := validator.ValidateAPIKey(r.Context(), r.Header.Get("X-Api-Key"))
		if err != nil {
			log.Error("API key validation failed", "err", err)
			writeError(w, http.StatusInternalServerError, codeInternal, "internal error")
			return
		}
		if !valid {
			writeError(w, http.StatusUnauthorized, codeUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}
