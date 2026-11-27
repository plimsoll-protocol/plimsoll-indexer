package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

// replayer is the storage surface the replay command needs.
type replayer interface {
	ReplayRawEvents(ctx context.Context, batchSize int) (store.ReplayStats, error)
	Close()
}

// openReplayer opens the store; tests replace it.
var openReplayer = func(ctx context.Context, url string) (replayer, error) {
	return store.Open(ctx, url)
}

// runReplayCommand rebuilds the typed tables from raw_events with this
// binary's decoder. Run it after a decoder change, for example one that starts
// tracking an event the indexer used to drop:
//
//	DATABASE_URL=postgres://... plimsoll-indexer replay
func runReplayCommand(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURL := fs.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL (default $DATABASE_URL)")
	batch := fs.Int("batch", 500, "raw events applied per transaction")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: plimsoll-indexer replay [-database-url URL] [-batch N]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Decodes every stored raw event again with this binary's decoder and applies")
		fmt.Fprintln(stderr, "the typed rows. Rows that already exist are left as they are, so it is safe")
		fmt.Fprintln(stderr, "to run on a live database; the ingest cursor is not touched.")
		fmt.Fprintln(stderr)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *databaseURL == "" {
		fmt.Fprintln(stderr, "replay: set DATABASE_URL or -database-url")
		return 2
	}
	if *batch <= 0 {
		fmt.Fprintln(stderr, "replay: -batch must be positive")
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := openReplayer(ctx, *databaseURL)
	if err != nil {
		fmt.Fprintln(stderr, "replay:", err)
		return 1
	}
	defer db.Close()

	stats, err := db.ReplayRawEvents(ctx, *batch)
	if err != nil {
		fmt.Fprintf(stderr, "replay: %v (%d raw events applied before the error)\n", err, stats.Events)
		return 1
	}
	fmt.Fprintf(stdout, "replayed %d raw events: %d decoded into typed rows, %d not tracked by this decoder\n",
		stats.Events, stats.Decoded, stats.Events-stats.Decoded)
	return 0
}
