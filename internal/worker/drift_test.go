package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/metrics"
)

// fakeLedgerSource answers GetLatestLedger with a fixed sequence, or with an
// error when the endpoint is meant to be unreachable.
type fakeLedgerSource struct {
	ledger uint32
	err    error
}

func (f *fakeLedgerSource) GetLatestLedger(context.Context) (protocol.GetLatestLedgerResponse, error) {
	if f.err != nil {
		return protocol.GetLatestLedgerResponse{}, f.err
	}
	return protocol.GetLatestLedgerResponse{Sequence: f.ledger}, nil
}

func newDriftMonitor(endpoints map[string]LedgerSource, maxDrift uint32, logs *bytes.Buffer) (*DriftMonitor, *metrics.Registry) {
	registry := metrics.NewRegistry()
	return NewDriftMonitor(endpoints, maxDrift, registry, slog.New(slog.NewTextHandler(logs, nil))), registry
}

func TestDriftMonitorWarnsWhenEndpointsDiverge(t *testing.T) {
	var logs bytes.Buffer
	monitor, registry := newDriftMonitor(map[string]LedgerSource{
		"https://a.example": &fakeLedgerSource{ledger: 500},
		"https://b.example": &fakeLedgerSource{ledger: 511},
	}, 10, &logs)
	if err := monitor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if out := logs.String(); !strings.Contains(out, "rpc endpoints have drifted apart") || !strings.Contains(out, "drift_ledgers=11") {
		t.Errorf("logs = %q, want a warning about 11 ledgers of drift", out)
	}

	var buf bytes.Buffer
	if err := registry.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`plimsoll_rpc_last_seen_ledger{endpoint="https://a.example"} 500`,
		`plimsoll_rpc_last_seen_ledger{endpoint="https://b.example"} 511`,
		"plimsoll_rpc_ledger_drift_ledgers 11",
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("metrics =\n%s\nmissing %q", buf.String(), want)
		}
	}

	// A second tick with the same gap is not a new episode.
	logs.Reset()
	if err := monitor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "drifted apart") {
		t.Errorf("second tick logged the same drift again: %q", logs.String())
	}
}

func TestDriftMonitorAcceptsDriftWithinTheLimit(t *testing.T) {
	var logs bytes.Buffer
	monitor, registry := newDriftMonitor(map[string]LedgerSource{
		"https://a.example": &fakeLedgerSource{ledger: 500},
		"https://b.example": &fakeLedgerSource{ledger: 510},
	}, 10, &logs)
	if err := monitor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if logs.Len() != 0 {
		t.Errorf("logs = %q, want nothing while the endpoints are at the limit", logs.String())
	}
	var buf bytes.Buffer
	if err := registry.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "plimsoll_rpc_ledger_drift_ledgers 10") {
		t.Errorf("metrics =\n%s\nwant a drift of 10", buf.String())
	}
}

func TestDriftMonitorLogsRecoveryOnceEndpointsAgree(t *testing.T) {
	var logs bytes.Buffer
	newest := &fakeLedgerSource{ledger: 520}
	monitor, _ := newDriftMonitor(map[string]LedgerSource{
		"https://a.example": &fakeLedgerSource{ledger: 500},
		"https://b.example": newest,
	}, 5, &logs)
	if err := monitor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "drifted apart") {
		t.Fatalf("logs = %q, want the drift warning first", logs.String())
	}

	newest.ledger = 501
	logs.Reset()
	if err := monitor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "rpc endpoints agree again") || !strings.Contains(logs.String(), "drift_ledgers=1") {
		t.Errorf("logs = %q, want recovery logged with the remaining drift", logs.String())
	}
}

func TestDriftMonitorToleratesAnUnreachableEndpoint(t *testing.T) {
	var logs bytes.Buffer
	monitor, registry := newDriftMonitor(map[string]LedgerSource{
		"https://a.example": &fakeLedgerSource{ledger: 500},
		"https://b.example": &fakeLedgerSource{err: errors.New("connection refused")},
	}, 10, &logs)
	if err := monitor.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(logs.String(), "did not report its latest ledger") {
		t.Errorf("logs = %q, want the failing endpoint logged", logs.String())
	}
	var buf bytes.Buffer
	if err := registry.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "https://b.example") {
		t.Errorf("metrics =\n%s\nshould hold no sample for the endpoint that failed", buf.String())
	}
	if !strings.Contains(buf.String(), "plimsoll_rpc_ledger_drift_ledgers 0") {
		t.Errorf("metrics =\n%s\nwant no drift from the endpoints that answered", buf.String())
	}
}

func TestDriftMonitorFailsWhenNoEndpointAnswers(t *testing.T) {
	var logs bytes.Buffer
	monitor, _ := newDriftMonitor(map[string]LedgerSource{
		"https://a.example": &fakeLedgerSource{err: errors.New("boom")},
	}, 10, &logs)
	if err := monitor.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil, want an error when no endpoint answers")
	}
}

// One endpoint cannot drift from itself, but its ledger is still published.
func TestDriftMonitorPublishesASingleEndpoint(t *testing.T) {
	var logs bytes.Buffer
	monitor, registry := newDriftMonitor(map[string]LedgerSource{
		"https://a.example": &fakeLedgerSource{ledger: 700},
	}, 10, &logs)
	if err := monitor.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if logs.Len() != 0 {
		t.Errorf("logs = %q, want nothing with a single endpoint", logs.String())
	}
	var buf bytes.Buffer
	if err := registry.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `plimsoll_rpc_last_seen_ledger{endpoint="https://a.example"} 700`) {
		t.Errorf("metrics =\n%s\nwant the endpoint's ledger", buf.String())
	}
	if !strings.Contains(buf.String(), "plimsoll_rpc_ledger_drift_ledgers 0") {
		t.Errorf("metrics =\n%s\nwant no drift with one endpoint", buf.String())
	}
}
