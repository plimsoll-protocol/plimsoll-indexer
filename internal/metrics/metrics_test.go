package metrics

import (
<<<<<<< HEAD
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsRegistered(t *testing.T) {
	// Verify that each metric can be updated without error
	IngestLagLedgers.Set(10)
	ReportsPerMinute.Set(5.5)
	ActiveAssets.Set(42)
	DBQueryDuration.WithLabelValues("test_query").Observe(0.012)
	ReportsIngestedTotal.Inc()
	EventsIngestedTotal.WithLabelValues("asset_listed").Inc()
	IngestLastEventAgeSeconds.Set(60)
	HTTPRequestDuration.WithLabelValues("/v1/assets", "200").Observe(0.005)

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	found := make(map[string]bool)
	for _, mf := range mfs {
		found[mf.GetName()] = true
	}

	required := []string{
		"plimsoll_ingest_lag_ledgers",
		"plimsoll_reports_per_minute",
		"plimsoll_assets_active",
		"plimsoll_db_query_duration_seconds",
		"plimsoll_reports_ingested_total",
		"plimsoll_events_ingested_total",
		"plimsoll_ingest_last_event_age_seconds",
		"plimsoll_http_request_duration_seconds",
	}

	for _, name := range required {
		if !found[name] {
			t.Errorf("metric %q not found in gathered metrics", name)
		}
	}
}
	"io"
=======
	"bytes"
>>>>>>> pr-215
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
<<<<<<< HEAD
	"time"
)

func TestMetricsRecordIngestPostsAndRequests(t *testing.T) {
	m := New()
	m.SetIngestProgress(20, 25)
	m.SupplyPostAttempted()
	m.SupplyPostSucceeded()
	m.RecordLastSupplyPost("C123", time.Now().Add(-2*time.Second))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/assets/{sac}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	handler := m.Middleware(mux)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/assets/C123", nil))

	response := httptest.NewRecorder()
	m.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	result := response.Result()
	defer result.Body.Close()
	body, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatal(err)
	}
	output := string(body)
	for _, metric := range []string{
		`plimsoll_ingested_ledger 20`,
		`plimsoll_ingest_lag_ledgers 5`,
		`plimsoll_supply_posts_attempted_total 1`,
		`plimsoll_supply_posts_succeeded_total 1`,
		`plimsoll_supply_post_last_age_seconds{asset_sac="C123"}`,
		`plimsoll_http_requests_total{method="GET",path="GET /v1/assets/{sac}",status="202"} 1`,
		`plimsoll_http_request_duration_seconds_count{method="GET",path="GET /v1/assets/{sac}"}`,
	} {
		if !strings.Contains(output, metric) {
			t.Errorf("metrics output missing %q:\n%s", metric, output)
		}
	}
}

func TestIngestLagDoesNotGoNegative(t *testing.T) {
	m := New()
	m.SetIngestProgress(25, 20)
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "plimsoll_ingest_lag_ledgers" {
			if got := family.GetMetric()[0].GetGauge().GetValue(); got != 0 {
				t.Fatalf("ingest lag = %v, want 0", got)
			}
			return
		}
	}
	t.Fatal("ingest lag metric not registered")
=======
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
>>>>>>> pr-215
}
