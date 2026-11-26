package worker

import (
	"context"
	"log/slog"
	"math/big"
	"os"
	"strconv"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/webhook"
)

func GetSupplyDropThreshold() float64 {
	val := os.Getenv("SUPPLY_DROP_THRESHOLD")
	if val == "" {
		return 0.20
	}
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0.20
	}
	return f
}

func CheckSupplyDrop(ctx context.Context, dispatcher *webhook.Dispatcher, sac, prevSupplyStr, currSupplyStr string) {
	thresh := GetSupplyDropThreshold()
	prev, ok1 := new(big.Float).SetString(prevSupplyStr)
	curr, ok2 := new(big.Float).SetString(currSupplyStr)
	if !ok1 || !ok2 || prev.Sign() <= 0 {
		return
	}
	diff := new(big.Float).Sub(prev, curr)
	if diff.Sign() <= 0 {
		return
	}
	dropFrac := new(big.Float).Quo(diff, prev)
	floatVal, _ := dropFrac.Float64()
	if floatVal > thresh {
		slog.Warn("sudden supply drop detected", "sac", sac, "previous", prevSupplyStr, "current", currSupplyStr, "drop_fraction", floatVal)
		if dispatcher != nil {
			_ = dispatcher.SupplyDrop(ctx, sac, prevSupplyStr, currSupplyStr, floatVal)
		}
	}
}
