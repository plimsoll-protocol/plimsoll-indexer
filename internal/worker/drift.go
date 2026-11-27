package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/metrics"
)

// LedgerSource is the one RPC call the drift monitor makes: the latest ledger
// an endpoint reports.
type LedgerSource interface {
	GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error)
}

// DriftMonitor records the latest ledger each RPC endpoint reports and warns
// when the endpoints disagree by more than MaxDrift ledgers.
//
// Endpoints fall behind each other after a restart, or while one of them
// catches up, and an endpoint that answers GetEvents from a shorter ledger range
// hands the indexer stale events without saying so. Publishing the per-endpoint
// ledger and the spread between them makes that visible before it turns into
// missing data.
//
// Run is not safe for concurrent use; the scheduler calls it from one goroutine.
type DriftMonitor struct {
	// Endpoints maps an endpoint, as configured, to the client that asks it for
	// its latest ledger.
	Endpoints map[string]LedgerSource
	// MaxDrift is how many ledgers the endpoints may differ by before the
	// monitor warns. Zero warns on any difference.
	MaxDrift uint32
	Log      *slog.Logger

	lastSeen  *metrics.Gauge
	drift     *metrics.Gauge
	excessive bool
}

// NewDriftMonitor registers the drift gauges and returns a monitor for the
// endpoints. registry may be nil, in which case only logging happens.
func NewDriftMonitor(endpoints map[string]LedgerSource, maxDrift uint32, registry *metrics.Registry, log *slog.Logger) *DriftMonitor {
	if log == nil {
		log = slog.Default()
	}
	m := &DriftMonitor{Endpoints: endpoints, MaxDrift: maxDrift, Log: log}
	if registry != nil {
		m.lastSeen = registry.NewGauge(
			"plimsoll_rpc_last_seen_ledger",
			"Most recent ledger each Soroban RPC endpoint reported.",
			"endpoint")
		m.drift = registry.NewGauge(
			"plimsoll_rpc_ledger_drift_ledgers",
			"Difference in ledgers between the newest and oldest Soroban RPC endpoint that answered.")
	}
	return m
}

// Run asks every endpoint for its latest ledger once, publishes what it saw and
// reports the spread. A warning is logged when the spread first exceeds
// MaxDrift, and recovery is logged once the endpoints agree again, so a
// sustained gap does not log on every tick.
func (m *DriftMonitor) Run(ctx context.Context) error {
	urls := make([]string, 0, len(m.Endpoints))
	for url := range m.Endpoints {
		urls = append(urls, url)
	}
	sort.Strings(urls)
	if len(urls) == 0 {
		return nil
	}

	type observation struct {
		endpoint string
		ledger   uint32
	}
	var seen []observation
	var errs []error
	for _, url := range urls {
		resp, err := m.Endpoints[url].GetLatestLedger(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", url, err))
			m.Log.Warn("rpc endpoint did not report its latest ledger", "endpoint", url, "error", err)
			continue
		}
		seen = append(seen, observation{endpoint: url, ledger: resp.Sequence})
		if m.lastSeen != nil {
			m.lastSeen.Set(float64(resp.Sequence), url)
		}
	}
	if len(seen) == 0 {
		return fmt.Errorf("no RPC endpoint reported its latest ledger: %w", errors.Join(errs...))
	}
	if len(seen) < 2 {
		// One endpoint cannot drift from itself; record that there is no gap.
		m.setDrift(0)
		return nil
	}

	newest, oldest := seen[0], seen[0]
	for _, o := range seen[1:] {
		if o.ledger > newest.ledger {
			newest = o
		}
		if o.ledger < oldest.ledger {
			oldest = o
		}
	}
	drift := newest.ledger - oldest.ledger
	m.setDrift(drift)

	if drift <= m.MaxDrift {
		if m.excessive {
			m.excessive = false
			m.Log.Info("rpc endpoints agree again", "drift_ledgers", drift, "max_drift_ledgers", m.MaxDrift)
		}
		return nil
	}
	if m.excessive {
		return nil
	}
	m.excessive = true
	m.Log.Warn("rpc endpoints have drifted apart",
		"drift_ledgers", drift, "max_drift_ledgers", m.MaxDrift,
		"newest_endpoint", newest.endpoint, "newest_ledger", newest.ledger,
		"oldest_endpoint", oldest.endpoint, "oldest_ledger", oldest.ledger)
	return nil
}

func (m *DriftMonitor) setDrift(drift uint32) {
	if m.drift != nil {
		m.drift.Set(float64(drift))
	}
}
