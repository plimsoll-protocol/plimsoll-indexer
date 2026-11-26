package coverage

import "time"

// Webhook is an integrator's coverage-threshold registration. It fires when an
// asset's basis points fall below MinBps using a report of at least MinTier.
type Webhook struct {
	ID        int64     `json:"id"`
	URL       string    `json:"url"`
	SAC       string    `json:"sac"`
	MinBps    uint32    `json:"min_bps"`
	MinTier   uint32    `json:"min_tier"`
	CreatedAt time.Time `json:"created_at"`
	// Alerted is true while the asset is below the threshold, so one crossing
	// produces one alert. It is cleared when coverage recovers, re-arming the
	// webhook for the next drop.
	Alerted bool `json:"-"`
}

// WebhookDelivery is one queued alert POST. Attempts counts the delivery
// attempts already made, including failures.
type WebhookDelivery struct {
	ID        int64
	WebhookID int64
	// URL is the webhook's target, joined in when the delivery is read.
	URL      string
	Payload  []byte
	Attempts int
}
