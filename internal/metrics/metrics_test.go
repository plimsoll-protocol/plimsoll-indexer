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
