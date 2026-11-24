package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
)

func TestWebhookRegistrationLifecycle(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()

	w, err := s.CreateWebhook(ctx, coverage.Webhook{
		URL: "https://integrator.example/hook", SAC: "CSAC", MinBps: 9_500, MinTier: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.ID <= 0 || w.CreatedAt.IsZero() || w.Alerted {
		t.Fatalf("created webhook = %+v", w)
	}

	hooks, err := s.ListWebhooks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(hooks) != 1 {
		t.Fatalf("ListWebhooks returned %d hooks, want 1", len(hooks))
	}
	got := hooks[0]
	if got.ID != w.ID || got.URL != w.URL || got.SAC != "CSAC" || got.MinBps != 9_500 || got.MinTier != 2 {
		t.Fatalf("listed webhook = %+v", got)
	}

	if err := s.SetWebhookAlerted(ctx, w.ID, true); err != nil {
		t.Fatal(err)
	}
	hooks, err = s.ListWebhooks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hooks[0].Alerted {
		t.Fatal("alerted was not stored")
	}

	if err := s.DeleteWebhook(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteWebhook(ctx, w.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete err = %v, want ErrNotFound", err)
	}
	hooks, err = s.ListWebhooks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(hooks) != 0 {
		t.Fatalf("ListWebhooks returned %d hooks after delete, want 0", len(hooks))
	}
}

func TestWebhookDeliveryLifecycle(t *testing.T) {
	s, url := openStore(t)
	ctx := context.Background()

	w, err := s.CreateWebhook(ctx, coverage.Webhook{
		URL: "https://integrator.example/hook", SAC: "CSAC", MinBps: 9_500, MinTier: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"event":"coverage_dropped","webhook_id":1,"bps":9000}`)
	if err := s.EnqueueWebhookDelivery(ctx, w.ID, payload); err != nil {
		t.Fatal(err)
	}

	due, err := s.DueWebhookDeliveries(ctx, 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 {
		t.Fatalf("due deliveries = %d, want 1", len(due))
	}
	d := due[0]
	if d.WebhookID != w.ID || d.URL != w.URL || d.Attempts != 0 {
		t.Fatalf("due delivery = %+v", d)
	}
	var echoed struct {
		Event string `json:"event"`
		Bps   uint32 `json:"bps"`
	}
	if err := json.Unmarshal(d.Payload, &echoed); err != nil {
		t.Fatalf("payload %q: %v", d.Payload, err)
	}
	if echoed.Event != "coverage_dropped" || echoed.Bps != 9000 {
		t.Fatalf("payload = %+v", echoed)
	}

	// A failed attempt pushes the next try into the future.
	if err := s.RecordWebhookAttempt(ctx, d.ID, 500, "status 500", false, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if due, err = s.DueWebhookDeliveries(ctx, 10, 5); err != nil {
		t.Fatal(err)
	} else if len(due) != 0 {
		t.Fatalf("delivery is due %d times during backoff, want 0", len(due))
	}

	// Once the backoff has passed it is due again, with the attempt counted.
	if err := s.RecordWebhookAttempt(ctx, d.ID, 0, "dial tcp: refused", false, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	due, err = s.DueWebhookDeliveries(ctx, 10, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Attempts != 2 {
		t.Fatalf("due = %+v, want one delivery with 2 attempts", due)
	}
	if due, err = s.DueWebhookDeliveries(ctx, 10, 2); err != nil {
		t.Fatal(err)
	} else if len(due) != 0 {
		t.Fatalf("delivery is due with attempts at the cap, want 0")
	}

	// A successful attempt ends the retries.
	if err := s.RecordWebhookAttempt(ctx, d.ID, 200, "", true, time.Now()); err != nil {
		t.Fatal(err)
	}
	if due, err = s.DueWebhookDeliveries(ctx, 10, 5); err != nil {
		t.Fatal(err)
	} else if len(due) != 0 {
		t.Fatalf("delivered delivery is still due, want 0")
	}

	var attempts, status int
	var deliveredAt *time.Time
	err = testPool(t, url).QueryRow(ctx, `
		SELECT attempts, COALESCE(last_status, 0), delivered_at
		FROM webhook_deliveries WHERE id = $1`, d.ID).Scan(&attempts, &status, &deliveredAt)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || status != 200 || deliveredAt == nil {
		t.Fatalf("delivery log: attempts=%d status=%d delivered_at=%v", attempts, status, deliveredAt)
	}
}
