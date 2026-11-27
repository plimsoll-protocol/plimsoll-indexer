package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/config"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestComponentLoggersIndependentLevels(t *testing.T) {
	var out syncBuffer
	loggers := newComponentLoggers(&out, config.LogLevels{
		Default: "info",
		API:     "debug",
		Worker:  "warn",
		Ingest:  "error",
	})

	loggers.api.Debug("api debug")
	loggers.worker.Info("worker info")
	loggers.worker.Warn("worker warn")
	loggers.ingest.Warn("ingest warn")
	loggers.ingest.Error("ingest error")

	got := out.String()
	for _, want := range []string{"api debug", "worker warn", "ingest error"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing expected log %q in %s", want, got)
		}
	}
	for _, unwanted := range []string{"worker info", "ingest warn"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("unexpected log %q in %s", unwanted, got)
		}
	}
}

func TestWatchLogLevelReloadOnSIGHUP(t *testing.T) {
	var out syncBuffer
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("LOG_LEVEL_API", "error")
	t.Setenv("LOG_LEVEL_WORKER", "")
	t.Setenv("LOG_LEVEL_INGEST", "")

	loggers := newComponentLoggers(&out, config.LoadLogLevels())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := watchLogLevelReload(ctx, loggers, config.LoadLogLevels)
	defer stop()

	loggers.api.Info("api info before sighup")
	if strings.Contains(out.String(), "api info before sighup") {
		t.Fatal("api info should be filtered at error level before SIGHUP")
	}

	// Update environment and send SIGHUP without restarting.
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("LOG_LEVEL_API", "debug")
	t.Setenv("LOG_LEVEL_WORKER", "warn")
	t.Setenv("LOG_LEVEL_INGEST", "error")

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatalf("send SIGHUP: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(out.String(), "reloaded log levels") {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for SIGHUP reload log; got: %s", out.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	loggers.api.Debug("api debug after sighup")
	loggers.worker.Info("worker info after sighup")
	loggers.worker.Warn("worker warn after sighup")
	loggers.ingest.Warn("ingest warn after sighup")
	loggers.ingest.Error("ingest error after sighup")

	got := out.String()
	for _, want := range []string{"api debug after sighup", "worker warn after sighup", "ingest error after sighup"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing expected log %q after SIGHUP; got: %s", want, got)
		}
	}
	for _, unwanted := range []string{"worker info after sighup", "ingest warn after sighup"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("unexpected log %q at higher level; got: %s", unwanted, got)
		}
	}

	// Clearing component vars falls back to LOG_LEVEL on subsequent SIGHUP.
	t.Setenv("LOG_LEVEL", "warn")
	t.Setenv("LOG_LEVEL_WORKER", "")
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatalf("send second SIGHUP: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for loggers.workerLevel.Level() != slog.LevelWarn || loggers.baseLevel.Level() != slog.LevelWarn {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for fallback reload; worker=%v base=%v",
				loggers.workerLevel.Level(), loggers.baseLevel.Level())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
