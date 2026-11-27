package main

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestJobGroupWaitsForSlowJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobs := &jobGroup{}
	release := make(chan struct{})
	log := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	jobs.startEvery(ctx, "slow", time.Hour, log, func(context.Context) error {
		<-release
		return nil
	})

	select {
	case <-release:
		t.Fatal("job unexpectedly started")
	default:
	}

	cancel()
	waited := make(chan struct{})
	go func() {
		jobs.wait(time.Second, log)
		close(waited)
	}()

	select {
	case <-waited:
		t.Fatal("wait returned before the slow job finished")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("wait did not return after the slow job finished")
	}
}

func TestJobGroupDeadlineReportsRunningJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobs := &jobGroup{}
	release := make(chan struct{})
	var logOutput bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logOutput, nil))

	jobs.startEvery(ctx, "slow", time.Hour, log, func(context.Context) error {
		<-release
		return nil
	})

	time.Sleep(10 * time.Millisecond)
	cancel()
	start := time.Now()
	jobs.wait(20*time.Millisecond, log)
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Fatalf("deadline wait returned too early: %v", elapsed)
	}
	if !bytes.Contains(logOutput.Bytes(), []byte("shutdown deadline reached")) {
		t.Fatalf("deadline log missing: %s", logOutput.String())
	}
	if !bytes.Contains(logOutput.Bytes(), []byte("slow")) {
		t.Fatalf("deadline log did not name slow job: %s", logOutput.String())
	}

	close(release)
	jobs.wait(time.Second, log)
}
