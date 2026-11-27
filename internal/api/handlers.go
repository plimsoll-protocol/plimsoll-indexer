package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/webhook"
)

type apiStore interface {
	GetAssetReporters(context.Context, string) ([]store.Reporter, error)
	GetWebhook(context.Context, int64) (store.Webhook, error)
	StreamAuditTrail(context.Context, func(store.AuditRecord) error) error
	StreamCSVExports(context.Context, func(store.ExportRecord) error) error
}

type Server struct {
	Store      apiStore
	Dispatcher *webhook.Dispatcher
	testLimits sync.Map
}

func (s *Server) ListAssetReporters(w http.ResponseWriter, r *http.Request) {
	sac := chi.URLParam(r, "sac")
	reporters, err := s.Store.GetAssetReporters(r.Context(), sac)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reporters)
}

func (s *Server) TestWebhook(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID <= 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	lastAny, loaded := s.testLimits.Load(req.ID)
	if loaded {
		last := lastAny.(time.Time)
		if time.Since(last) < 60*time.Second {
			http.Error(w, "rate limited: 1 test per webhook per 60s", http.StatusTooManyRequests)
			return
		}
	}
	hook, err := s.Store.GetWebhook(r.Context(), req.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "webhook not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	status, err := s.Dispatcher.DeliverTest(r.Context(), hook)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	s.testLimits.Store(req.ID, time.Now())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"status": status})
}

func (s *Server) StreamAudit(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Accept") == "application/x-ndjson" {
		w.Header().Set("Content-Type", "application/x-ndjson")
		flusher, _ := w.(http.Flusher)
		err := s.Store.StreamAuditTrail(r.Context(), func(rec store.AuditRecord) error {
			data, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			if _, err := w.Write(append(data, '\n')); err != nil {
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
			return nil
		})
		if err != nil {
			// Log error if needed
		}
		return
	}
	// Fallback normal response
	w.WriteHeader(http.StatusNotAcceptable)
}

func (s *Server) StreamCSV(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Accept") == "application/x-ndjson" {
		w.Header().Set("Content-Type", "application/x-ndjson")
		flusher, _ := w.(http.Flusher)
		err := s.Store.StreamCSVExports(r.Context(), func(rec store.ExportRecord) error {
			data, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			if _, err := w.Write(append(data, '\n')); err != nil {
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
			return nil
		})
		if err != nil {
		}
		return
	}
	w.WriteHeader(http.StatusNotAcceptable)
}
