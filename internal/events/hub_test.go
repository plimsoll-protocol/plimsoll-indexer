package events

import (
	"testing"
	"time"
)

func TestPublishDeliversInOrder(t *testing.T) {
	h := NewHub(2)
	sub, ok := h.Subscribe()
	if !ok {
		t.Fatal("subscribe refused")
	}
	defer sub.Close()

	h.Publish([]any{"first", "second", "third"})

	for _, want := range []string{"first", "second", "third"} {
		select {
		case got := <-sub.Events():
			if got != want {
				t.Fatalf("got %v, want %s", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s never arrived", want)
		}
	}
}

func TestSubscribeRespectsCap(t *testing.T) {
	h := NewHub(1)
	first, ok := h.Subscribe()
	if !ok {
		t.Fatal("first subscription refused")
	}
	if _, ok := h.Subscribe(); ok {
		t.Fatal("second subscription accepted past the cap")
	}
	if h.Subscribers() != 1 {
		t.Fatalf("subscribers = %d, want 1", h.Subscribers())
	}
	first.Close()
	// Closing is idempotent and frees the slot.
	first.Close()
	if h.Subscribers() != 0 {
		t.Fatalf("subscribers after close = %d, want 0", h.Subscribers())
	}
	if _, ok := h.Subscribe(); !ok {
		t.Fatal("subscription refused after a slot was freed")
	}
}

// A client that stops reading must not hold up ingestion: the hub drops it and
// Publish returns immediately. The channel is closed so the handler returns.
func TestPublishDropsSlowSubscriber(t *testing.T) {
	h := NewHub(1)
	sub, ok := h.Subscribe()
	if !ok {
		t.Fatal("subscribe refused")
	}
	defer sub.Close()

	done := make(chan struct{})
	go func() {
		for i := 0; i <= buffer; i++ {
			h.Publish([]any{i})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a subscriber that was not reading")
	}

	if h.Subscribers() != 0 {
		t.Fatalf("slow subscriber still registered (%d)", h.Subscribers())
	}
	drained := 0
	for range sub.Events() {
		drained++
	}
	if drained != buffer {
		t.Fatalf("drained %d buffered events, want %d", drained, buffer)
	}
}

// Closing a subscription while Publish is running must not panic on a double
// close; run with -race to catch the data race too.
func TestCloseDuringPublish(t *testing.T) {
	h := NewHub(4)
	sub, ok := h.Subscribe()
	if !ok {
		t.Fatal("subscribe refused")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			h.Publish([]any{i})
		}
	}()
	go func() {
		for i := 0; i < 100; i++ {
			sub.Close()
		}
	}()
	<-done
	sub.Close()
	if h.Subscribers() != 0 {
		t.Fatalf("subscribers = %d, want 0", h.Subscribers())
	}
}
