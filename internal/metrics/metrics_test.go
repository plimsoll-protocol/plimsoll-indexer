package metrics

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWritePrometheusRendersGauges(t *testing.T) {
	r := NewRegistry()
	lastSeen := r.NewGauge(
		"plimsoll_rpc_last_seen_ledger",
		"Most recent ledger each Soroban RPC endpoint reported.",
		"endpoint")
	lastSeen.Set(120, "https://b.example")
	lastSeen.Set(118, `https://a.example/"quoted"`)
	drift := r.NewGauge(
		"plimsoll_rpc_ledger_drift_ledgers",
		"Difference in ledgers between the newest and oldest endpoint that answered.")
	drift.Set(2)

	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	want := `# HELP plimsoll_rpc_last_seen_ledger Most recent ledger each Soroban RPC endpoint reported.
# TYPE plimsoll_rpc_last_seen_ledger gauge
plimsoll_rpc_last_seen_ledger{endpoint="https://a.example/\"quoted\""} 118
plimsoll_rpc_last_seen_ledger{endpoint="https://b.example"} 120
# HELP plimsoll_rpc_ledger_drift_ledgers Difference in ledgers between the newest and oldest endpoint that answered.
# TYPE plimsoll_rpc_ledger_drift_ledgers gauge
plimsoll_rpc_ledger_drift_ledgers 2
`
	if buf.String() != want {
		t.Errorf("exposition =\n%s\nwant\n%s", buf.String(), want)
	}
}

// Setting the same label set again replaces the value instead of adding a
// second sample, which is what a gauge that is set every tick needs.
func TestGaugeSetReplacesSample(t *testing.T) {
	r := NewRegistry()
	g := r.NewGauge("plimsoll_rpc_last_seen_ledger", "help", "endpoint")
	g.Set(1, "https://rpc.example")
	g.Set(9, "https://rpc.example")

	var buf bytes.Buffer
	if err := r.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	body := buf.String()
	if strings.Count(body, "plimsoll_rpc_last_seen_ledger{") != 1 || !strings.Contains(body, "} 9\n") {
		t.Errorf("exposition =\n%s\nwant one sample holding 9", body)
	}
}

func TestHandlerServesTextExposition(t *testing.T) {
	r := NewRegistry()
	r.NewGauge("plimsoll_rpc_ledger_drift_ledgers", "help").Set(3)

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain; version=0.0.4") {
		t.Errorf("Content-Type = %q", got)
	}
	if !strings.Contains(rec.Body.String(), "plimsoll_rpc_ledger_drift_ledgers 3\n") {
		t.Errorf("body = %q", rec.Body.String())
	}
}
