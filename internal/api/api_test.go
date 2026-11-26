package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/webhook"
)

type mockAPIStore struct {
	reporters []store.Reporter
	webhook   store.Webhook
	audit     []store.AuditRecord
}

func (m mockAPIStore) GetAssetReporters(ctx context.Context, sac string) ([]store.Reporter, error) {
	return m.reporters, nil
}
func (m mockAPIStore) GetWebhook(ctx context.Context, id int64) (store.Webhook, error) {
	if id == m.webhook.ID {
		return m.webhook, nil
	}
	return store.Webhook{}, store.ErrNotFound
}
func (m mockAPIStore) StreamAuditTrail(ctx context.Context, fn func(store.AuditRecord) error) error {
	for _, r := range m.audit {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}
func (m mockAPIStore) StreamCSVExports(ctx context.Context, fn func(store.ExportRecord) error) error {
	return nil
}

func TestListAssetReportersHandler(t *testing.T) {
	rep := []store.Reporter{
		{Reporter: "G1", Tier: 2, LastPostAt: time.Now()},
		{Reporter: "G2", Tier: 1, LastPostAt: time.Now()},
	}
	s := &Server{Store: mockAPIStore{reporters: rep}}
	r := chi.NewRouter()
	r.Get("/v1/assets/{sac}/reporters", s.ListAssetReporters)

	req := httptest.NewRequest(http.MethodGet, "/v1/assets/SAC1/reporters", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		Fatalf := t.Fatalf
		Fatalf("status = %d", rec.Code)
	}
}

func TestStreamAuditNDJSON(t *testing.T) {
	s := &Server{Store: mockAPIStore{audit: []store.AuditRecord{{ID: 1, Action: "test", Timestamp: "now"}}}}
	r := chi.NewRouter()
	r.Get("/v1/audit", s.StreamAudit)

	req := httptest.NewRequest(http.MethodGet, "/v1/audit", nil)
	req.Header.Set("Accept", "application/x-ndjson")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("status=%d content-type=%q", rec.Code, rec.Header().Get("Content-Type"))
	}
}
