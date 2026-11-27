package metrics

import (
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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
}
