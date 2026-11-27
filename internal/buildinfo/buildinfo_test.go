package buildinfo

import (
	"runtime"
	"testing"
)

func TestGetUsesInjectedValues(t *testing.T) {
	oldVersion, oldCommit := Version, Commit
	t.Cleanup(func() { Version, Commit = oldVersion, oldCommit })
	Version, Commit = "v1.2.3", "abc123"

	info := Get()
	if info.Version != "v1.2.3" {
		t.Errorf("Version = %q", info.Version)
	}
	if info.Commit != "abc123" {
		t.Errorf("Commit = %q", info.Commit)
	}
	if info.Go != runtime.Version() {
		t.Errorf("Go = %q want %q", info.Go, runtime.Version())
	}
}

func TestGetFallsBackWithoutInjectedValues(t *testing.T) {
	oldVersion, oldCommit := Version, Commit
	t.Cleanup(func() { Version, Commit = oldVersion, oldCommit })
	// Use an explicit empty Version and a commit the build cannot know, so
	// only the fallback paths can fill them.
	Version, Commit = "", ""

	info := Get()
	if info.Version == "" {
		t.Error("Version is empty")
	}
	if info.Commit == "" {
		t.Error("Commit is empty")
	}
	if info.Go == "" {
		t.Error("Go is empty")
	}
}
