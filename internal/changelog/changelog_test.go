package changelog

import (
	"strings"
	"testing"
)

func TestLoadEmbeddedChangelog(t *testing.T) {
	entries, err := Load()
	if err != nil {
		t.Fatalf("embedded CHANGELOG.yaml: %v", err)
	}
	if len(entries) == 0 || entries[0].Version == "" || entries[0].Date == "" {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestParse(t *testing.T) {
	entries, err := Parse([]byte(`
- version: "1.2.0"
  date: "2026-12-01"
  changes:
    - kind: removed_field
      detail: "coverage.ratio"
    - kind: new_required_param
      detail: "min_tier is required on /v1/assets"
- version: "1.1.0"
  date: "2026-11-01"
  changes:
    - kind: renamed_endpoint
      detail: "/v1/reserves is now /v1/reports"
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Version != "1.2.0" || len(entries[0].Changes) != 2 ||
		entries[0].Changes[0].Kind != "removed_field" || entries[1].Changes[0].Detail == "" {
		t.Fatalf("entries = %+v", entries)
	}
}

func TestParseRejectsMalformedChangelog(t *testing.T) {
	for name, tc := range map[string]struct {
		yaml string
		want string
	}{
		"empty":     {"[]", "no versions"},
		"not yaml":  {"- version: [", "parse changelog"},
		"no semver": {"- version: \"1.0\"\n  date: \"2026-10-05\"\n  changes: []", "is not MAJOR.MINOR.PATCH"},
		"bad date":  {"- version: \"1.0.0\"\n  date: \"October\"\n  changes: []", "date must be YYYY-MM-DD"},
		"oldest first": {`- version: "1.0.0"
  date: "2026-10-05"
  changes: []
- version: "1.1.0"
  date: "2026-11-01"
  changes: []`, "is not older than the version above it"},
		"duplicate version": {`- version: "1.0.0"
  date: "2026-10-05"
  changes: []
- version: "1.0.0"
  date: "2026-09-05"
  changes: []`, "appears twice"},
		"change without detail": {`- version: "1.0.0"
  date: "2026-10-05"
  changes:
    - kind: removed_field`, "needs a kind and a detail"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}
