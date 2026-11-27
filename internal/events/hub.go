// Package events fans newly ingested Plimsoll events out to live API
// subscribers without letting a slow reader hold up ingestion.
package events

import "sync"

const (
	// DefaultMaxSubscribers bounds how many live streams one instance serves.
	// Each subscriber costs a connection, a goroutine and a small buffered
	// channel; past this cap clients get 503 and can fall back to polling.
	DefaultMaxSubscribers = 128

	// buffer is how far one subscriber may fall behind before it is dropped.
	// Nothing is lost by dropping: the stream resumes from storage using
	// Last-Event-ID. The buffer is larger than the API's replay batch so a
	// burst during a replay does not cost the client its connection.
	buffer = 256
)

// Hub is an in-process broadcaster. Publishing never blocks: a subscriber that
// cannot keep up is disconnected instead, so a stalled HTTP client cannot
// delay the ingest loop.
type Hub struct {
	maxSubscribers int

	mu   sync.Mutex
	subs map[*Subscription]struct{}
}

// NewHub returns a hub that accepts at most maxSubscribers live streams.
// A non-positive value uses DefaultMaxSubscribers.
func NewHub(maxSubscribers int) *Hub {
	if maxSubscribers <= 0 {
		maxSubscribers = DefaultMaxSubscribers
	}
	return &Hub{maxSubscribers: maxSubscribers, subs: make(map[*Subscription]struct{})}
}

// Subscription is one live stream. Events is closed when the subscription is
// closed or dropped for falling behind.
type Subscription struct {
	hub *Hub
	ch  chan any

	// closed is guarded by hub.mu so Close and a drop cannot both close ch.
	closed bool
}

// Subscribe registers a new subscriber. It reports false when the hub is full.
func (h *Hub) Subscribe() (*Subscription, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.subs) >= h.maxSubscribers {
		return nil, false
	}
	sub := &Subscription{hub: h, ch: make(chan any, buffer)}
	h.subs[sub] = struct{}{}
	return sub, true
}

// Events returns the channel live events are published to.
func (s *Subscription) Events() <-chan any { return s.ch }

// Close unsubscribes. It is safe to call more than once.
func (s *Subscription) Close() { s.hub.unsubscribe(s) }

// Publish delivers events to every subscriber in order. A subscriber whose
// buffer is full is dropped rather than waited for, so Publish always returns
// promptly no matter how slow its clients are.
func (h *Hub) Publish(events []any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ev := range events {
		for sub := range h.subs {
			select {
			case sub.ch <- ev:
			default:
				h.dropLocked(sub)
			}
		}
	}
}

// Subscribers returns how many live streams the hub is serving.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

func (h *Hub) unsubscribe(s *Subscription) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dropLocked(s)
}

// dropLocked removes sub and closes its channel. The channel is only closed
// here, under the hub lock, so a subscriber is never closed twice.
func (h *Hub) dropLocked(sub *Subscription) {
	if sub.closed {
		return
	}
	sub.closed = true
	delete(h.subs, sub)
	close(sub.ch)
}
