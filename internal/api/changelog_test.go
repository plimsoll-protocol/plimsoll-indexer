package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/changelog"
)

// testChangelog is what the test server serves at /v1/changelog.
var testChangelog = []changelog.Entry{{
	Version: "1.2.0",
	Date:    "2026-11-20",
	Changes: []changelog.Change{{
		Kind:   "removed_field",
		Detail: "coverage.ratio was removed; compute it from bps.",
	}},
}}

func TestChangelogEndpoint(t *testing.T) {
	srv := server()
	defer srv.Close()

	body := get(t, srv, "/v1/changelog", 200)
	entries, ok := body["changelog"].([]any)
	if !ok || len(entries) != 1 {
		t.Fatalf("changelog = %#v", body["changelog"])
	}
	entry := entries[0].(map[string]any)
	if entry["version"] != "1.2.0" || entry["date"] != "2026-11-20" {
		t.Fatalf("entry = %#v", entry)
	}
	changes, ok := entry["changes"].([]any)
	if !ok || len(changes) != 1 {
		t.Fatalf("changes = %#v", entry["changes"])
	}
	change := changes[0].(map[string]any)
	if change["kind"] != "removed_field" || change["detail"] == "" {
		t.Fatalf("change = %#v", change)
	}
}

// Without a changelog loaded the endpoint still answers with a JSON array, so
// clients never have to special-case null.
func TestChangelogEmpty(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.changelog(rec, httptest.NewRequest(http.MethodGet, "/v1/changelog", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Changelog []changelog.Entry `json:"changelog"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	if body.Changelog == nil || len(body.Changelog) != 0 {
		t.Fatalf("changelog = %#v, want an empty array", body.Changelog)
	}
}
