package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

type fakeReplayer struct {
	batch  int
	stats  store.ReplayStats
	err    error
	closed bool
}

func (f *fakeReplayer) ReplayRawEvents(_ context.Context, batchSize int) (store.ReplayStats, error) {
	f.batch = batchSize
	return f.stats, f.err
}

func (f *fakeReplayer) Close() { f.closed = true }

func withReplayer(t *testing.T, f *fakeReplayer, openErr error) *string {
	t.Helper()
	var opened string
	prev := openReplayer
	openReplayer = func(_ context.Context, url string) (replayer, error) {
		opened = url
		if openErr != nil {
			return nil, openErr
		}
		return f, nil
	}
	t.Cleanup(func() { openReplayer = prev })
	return &opened
}

func TestReplayCommand(t *testing.T) {
	f := &fakeReplayer{stats: store.ReplayStats{Events: 6, Decoded: 5}}
	opened := withReplayer(t, f, nil)
	t.Setenv("DATABASE_URL", "postgres://env/db")

	var out, errOut bytes.Buffer
	if code := runReplayCommand([]string{"-batch", "50"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	if *opened != "postgres://env/db" || f.batch != 50 || !f.closed {
		t.Fatalf("opened %q, batch %d, closed %v", *opened, f.batch, f.closed)
	}
	if want := "replayed 6 raw events: 5 decoded into typed rows, 1 not tracked by this decoder"; !strings.Contains(out.String(), want) {
		t.Fatalf("output %q, want %q", out.String(), want)
	}

	// The flag wins over the environment.
	if code := runReplayCommand([]string{"-database-url", "postgres://flag/db"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if *opened != "postgres://flag/db" || f.batch != 500 {
		t.Fatalf("opened %q with batch %d, want the flag URL and the default batch", *opened, f.batch)
	}
}

func TestReplayCommandFailures(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	var out, errOut bytes.Buffer
	if code := runReplayCommand(nil, &out, &errOut); code != 2 || !strings.Contains(errOut.String(), "DATABASE_URL") {
		t.Fatalf("no database: exit %d, %q", code, errOut.String())
	}

	t.Setenv("DATABASE_URL", "postgres://env/db")
	errOut.Reset()
	if code := runReplayCommand([]string{"-batch", "0"}, &out, &errOut); code != 2 {
		t.Fatalf("batch 0: exit %d", code)
	}

	withReplayer(t, nil, errors.New("connect: refused"))
	errOut.Reset()
	if code := runReplayCommand(nil, &out, &errOut); code != 1 || !strings.Contains(errOut.String(), "connect: refused") {
		t.Fatalf("open failure: exit %d, %q", code, errOut.String())
	}

	f := &fakeReplayer{stats: store.ReplayStats{Events: 400}, err: errors.New("replay: event 0000000099-0000000001: bad topic")}
	withReplayer(t, f, nil)
	errOut.Reset()
	code := runReplayCommand(nil, &out, &errOut)
	if code != 1 || !strings.Contains(errOut.String(), "0000000099-0000000001") || !strings.Contains(errOut.String(), "400 raw events applied") {
		t.Fatalf("replay failure: exit %d, %q", code, errOut.String())
	}
	if !f.closed {
		t.Fatal("store left open after a failed replay")
	}
}
