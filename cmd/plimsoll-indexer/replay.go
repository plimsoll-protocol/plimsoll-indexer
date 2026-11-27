package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/config"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/netclient"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/worker"
)

// replayer is the storage surface the raw-event replay needs.
type replayer interface {
	ReplayRawEvents(ctx context.Context, batchSize int) (store.ReplayStats, error)
	Close()
}

// openReplayer opens the store; tests replace it.
var openReplayer = func(ctx context.Context, url string) (replayer, error) {
	return store.Open(ctx, url)
}

// runReplayCommand has two modes:
//
// Without a range it decodes every stored raw event again with this binary's
// decoder and applies the typed rows, which is what a decoder change needs:
//
//	DATABASE_URL=postgres://... plimsoll-indexer replay
//
// With -from and -to it re-reads that ledger range from the RPC and upserts
// what it finds, which fills in events the indexer missed or decoded before a
// fix. Neither mode touches the ingest cursor, so both are safe on a live
// database.
func runReplayCommand(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURL := fs.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL (default $DATABASE_URL)")
	batch := fs.Int("batch", 500, "raw events applied per transaction (whole-history replay)")
	from := fs.Uint64("from", 0, "first ledger to re-ingest from the RPC (needs -to)")
	to := fs.Uint64("to", 0, "last ledger to re-ingest from the RPC (needs -from)")
	dryRun := fs.Bool("dry-run", false, "with -from/-to: report what would be applied without writing")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: plimsoll-indexer replay [-database-url URL] [-batch N]")
		fmt.Fprintln(stderr, "       plimsoll-indexer replay -from N -to M [-dry-run]")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "Without -from/-to it re-decodes every stored raw event with this binary's")
		fmt.Fprintln(stderr, "decoder and applies the typed rows. With -from/-to it fetches that ledger")
		fmt.Fprintln(stderr, "range from the RPC and upserts the events it contains. Rows that already")
		fmt.Fprintln(stderr, "exist are left as they are and the ingest cursor is not touched, so either")
		fmt.Fprintln(stderr, "mode is safe to run on a live database.")
		fmt.Fprintln(stderr)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *from != 0 || *to != 0 || *dryRun {
		return runRangeReplay(*from, *to, *dryRun, *databaseURL, stdout, stderr)
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

// runRangeReplay re-ingests [from, to] from the RPC. A dry run reports the
// range without opening the database.
func runRangeReplay(from, to uint64, dryRun bool, databaseURL string, stdout, stderr io.Writer) int {
	switch {
	case from == 0:
		fmt.Fprintln(stderr, "replay: -from is required with -to and must be greater than 0")
		return 2
	case to == 0:
		fmt.Fprintln(stderr, "replay: -to is required with -from and must be greater than 0")
		return 2
	case to < from:
		fmt.Fprintf(stderr, "replay: -to (%d) must not be before -from (%d)\n", to, from)
		return 2
	case to > uint64(math.MaxUint32):
		fmt.Fprintf(stderr, "replay: -to (%d) is past the largest ledger sequence\n", to)
		return 2
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(stderr, "replay: invalid configuration: %v\n", err)
		return 1
	}
	log := newLogger(cfg.LogLevel).With("job", "replay")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rpc := rpcclient.NewClient(cfg.RPCURL, netclient.New(30*time.Second, cfg.HTTPKeepAlive))
	defer rpc.Close()

	// A dry run needs no database at all: it reports what the range holds
	// without writing anything.
	var db *store.Store
	if !dryRun {
		url := databaseURL
		if url == "" {
			url = cfg.DatabaseURL
		}
		db, err = store.Open(ctx, url)
		if err != nil {
			fmt.Fprintf(stderr, "replay: open database: %v\n", err)
			return 1
		}
		defer db.Close()
	}

	replayer := &worker.Replayer{
		RPC:         rpc,
		ContractIDs: []string{cfg.CoverageLedgerID, cfg.ReporterRegistryID},
		Log:         log,
	}
	if db != nil {
		replayer.Store = db
	}
	summary, err := replayer.Run(ctx, uint32(from), uint32(to), dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "replay: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout,
		"replayed ledgers %d-%d: %d assets, %d supplies, %d reports, %d reporter registrations, %d revocations\n",
		from, to, summary.Assets, summary.Supplies, summary.Reports, summary.Reporters, summary.Revoked)
	return 0
}
