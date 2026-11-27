package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

const (
	defaultEventHeartbeat    = 15 * time.Second
	defaultEventWriteTimeout = 30 * time.Second
	// replayBatch is how many stored events a resuming client is sent per
	// storage round trip.
	replayBatch = 200
)

// streamEvent is one server-sent event frame.
type streamEvent struct {
	ID   string
	Type string
	Data []byte
}

// events serves GET /v1/events as text/event-stream.
//
// A subscriber is registered before the backlog is read, so an event that
// lands during a replay is queued instead of lost; it is then skipped as a
// duplicate by id. A client that stops reading is dropped by the hub rather
// than delaying ingestion, and reconnects with Last-Event-ID to resume.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	if s.Events == nil {
		writeError(w, http.StatusServiceUnavailable, "event streaming is not configured")
		return
	}
	sub, ok := s.Events.Subscribe()
	if !ok {
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "too many event subscribers")
		return
	}
	defer sub.Close()

	writeTimeout := s.EventWriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultEventWriteTimeout
	}
	heartbeat := s.EventHeartbeat
	if heartbeat <= 0 {
		heartbeat = defaultEventHeartbeat
	}

	ctrl := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Proxies such as nginx buffer responses unless told not to, which would
	// hold every frame back until the connection closed.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := ctrl.Flush(); err != nil {
		return
	}

	lastID := r.Header.Get("Last-Event-ID")
	if lastID != "" {
		var err error
		lastID, err = s.replay(r.Context(), w, ctrl, lastID, writeTimeout)
		if err != nil {
			// The headers are already out, so the only honest signal left is
			// closing the connection: the client reconnects and resumes from
			// the last frame it received.
			s.Log.Warn("event replay failed, closing stream", "last_event_id", lastID, "err", err)
			return
		}
	}

	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.Done:
			// Graceful shutdown: a live stream would otherwise hold Shutdown
			// until its timeout.
			return
		case <-ticker.C:
			if err := writeComment(w, ctrl, writeTimeout); err != nil {
				return
			}
		case ev, ok := <-sub.Events():
			if !ok {
				// Dropped for falling behind. Reconnecting resumes from the
				// last id this stream sent.
				return
			}
			frame, ok := renderEvent(ev)
			if !ok {
				continue
			}
			// Replayed and live delivery can overlap; the id is the same event
			// id the chain assigned, so one comparison removes the duplicate.
			if frame.ID != "" && lastID != "" && frame.ID <= lastID {
				continue
			}
			if err := writeFrame(w, ctrl, writeTimeout, frame); err != nil {
				return
			}
			if frame.ID != "" {
				lastID = frame.ID
			}
		}
	}
}

// replay writes every stored event after afterID and returns the id of the
// last frame sent.
func (s *Server) replay(ctx context.Context, w http.ResponseWriter, ctrl *http.ResponseController, afterID string, timeout time.Duration) (string, error) {
	for {
		stored, err := s.Store.EventsSince(ctx, afterID, replayBatch)
		if err != nil {
			return afterID, err
		}
		sent := 0
		for _, ev := range stored {
			frame, ok := renderEvent(ev)
			if !ok || frame.ID == "" || frame.ID <= afterID {
				continue
			}
			if err := writeFrame(w, ctrl, timeout, frame); err != nil {
				return afterID, err
			}
			afterID = frame.ID
			sent++
		}
		if len(stored) < replayBatch || sent == 0 {
			return afterID, nil
		}
	}
}

// renderEvent gives an event the same JSON shape its REST endpoint uses and
// drops the events the stream does not carry.
func renderEvent(ev any) (streamEvent, bool) {
	var (
		id   string
		name string
		data []byte
		err  error
	)
	switch e := ev.(type) {
	case soroban.AssetListed:
		id = e.ID
		name = soroban.EventAssetListed
		data, err = json.Marshal(coverage.Asset{
			SAC:          e.SAC,
			Code:         e.Code,
			Issuer:       e.Issuer,
			ListedLedger: e.Ledger,
			ListedAt:     e.ClosedAt,
		})
	case soroban.SupplyPosted:
		id = e.ID
		name = soroban.EventSupplyPosted
		data, err = json.Marshal(toSupply(&coverage.Supply{
			Amount:        e.Amount,
			Ledger:        e.AtLedger,
			PostedAt:      e.ClosedAt,
			BreakdownHash: e.BreakdownHash,
			Poster:        e.Poster,
			TxHash:        e.TxHash,
			Source:        "event",
		}))
	case soroban.ReservePosted:
		id = e.ID
		name = soroban.EventReservePosted
		data, err = json.Marshal(toReport(coverage.Report{
			Tier:     e.Tier,
			Amount:   e.Amount,
			AsOf:     e.AsOf,
			PostedAt: e.ClosedAt,
			Reporter: e.Reporter,
			DocHash:  e.DocHash,
			DocURI:   e.DocURI,
			TxHash:   e.TxHash,
			Source:   "event",
		}))
	default:
		// reporter_set and reporter_revoked are not part of the stream.
		return streamEvent{}, false
	}
	if err != nil {
		return streamEvent{}, false
	}
	return streamEvent{ID: id, Type: name, Data: data}, true
}

func writeFrame(w http.ResponseWriter, ctrl *http.ResponseController, timeout time.Duration, frame streamEvent) error {
	payload := make([]byte, 0, len(frame.Data)+len(frame.ID)+len(frame.Type)+32)
	if frame.ID != "" {
		payload = append(payload, "id: "...)
		payload = append(payload, frame.ID...)
		payload = append(payload, '\n')
	}
	payload = append(payload, "event: "...)
	payload = append(payload, frame.Type...)
	payload = append(payload, '\n')
	// json.Marshal never emits a raw newline, so the payload is one data line.
	payload = append(payload, "data: "...)
	payload = append(payload, frame.Data...)
	payload = append(payload, '\n', '\n')
	return writeSSE(w, ctrl, timeout, payload)
}

// writeComment sends an SSE comment, which clients ignore but proxies and
// idle-connection timeouts treat as activity.
func writeComment(w http.ResponseWriter, ctrl *http.ResponseController, timeout time.Duration) error {
	return writeSSE(w, ctrl, timeout, []byte(": keepalive\n\n"))
}

func writeSSE(w http.ResponseWriter, ctrl *http.ResponseController, timeout time.Duration, payload []byte) error {
	// A client that stops reading would otherwise pin this goroutine and its
	// connection forever. Kernels without deadlines report ErrNotSupported,
	// which only costs that protection.
	if err := ctrl.SetWriteDeadline(time.Now().Add(timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return ctrl.Flush()
}
