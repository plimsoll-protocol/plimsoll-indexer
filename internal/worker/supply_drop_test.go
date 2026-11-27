package worker

import (
	"context"
	"testing"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/webhook"
)

func TestCheckSupplyDropTriggers(t *testing.T) {
	// Should trigger warning and call webhook if drop > 20%
	CheckSupplyDrop(context.Background(), nil, "SAC", "100", "70")
}
