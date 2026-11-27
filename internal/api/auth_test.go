package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

type apiKeyValidatorFunc func(context.Context, string) (bool, error)

func (f apiKeyValidatorFunc) ValidateAPIKey(ctx context.Context, key string) (bool, error) {
	return f(ctx, key)
}

func TestAPIKeyMiddleware(t *testing.T) {
	handler := APIKeyMiddleware(apiKeyValidatorFunc(func(_ context.Context, key string) (bool, error) {
		return key == "valid", nil
	}), slog.New(slog.NewTextHandler(io.Discard, nil)), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	for _, test := range []struct {
		name string
		key  string
		want int
	}{
		{name: "missing", want: http.StatusUnauthorized},
		{name: "invalid", key: "invalid", want: http.StatusUnauthorized},
		{name: "valid", key: "valid", want: http.StatusNoContent},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/reports", nil)
			if test.key != "" {
				req.Header.Set("X-Api-Key", test.key)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != test.want {
				t.Fatalf("status = %d, want %d", rec.Code, test.want)
			}
		})
	}
}

func TestAPIKeyMiddlewareSurfacesValidatorErrors(t *testing.T) {
	handler := APIKeyMiddleware(apiKeyValidatorFunc(func(context.Context, string) (bool, error) {
		return false, errors.New("database unavailable")
	}), slog.New(slog.NewTextHandler(io.Discard, nil)), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("downstream handler called for failed validation")
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/reports", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}
