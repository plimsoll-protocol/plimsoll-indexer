package main

import (
	"context"
	"flag"
	"fmt"
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

// runReplayCommand re-ingests a ledger range and exits 0 on success. It writes
// through ApplyReplayEvents, which never touches the ingest cursor, so it is
// safe to run against a live database.
func runReplayCommand(args []string) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	from := fs.Uint64("from", 0, "first ledger to replay (inclusive)")
	to := fs.Uint64("to", 0, "last ledger to replay (inclusive)")
	dryRun := fs.Bool("dry-run", false, "log what would be upserted without writing to the database")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: plimsoll-indexer replay --from N --to M [--dry-run]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 1
	}
	switch {
	case *from == 0:
		fmt.Fprintln(os.Stderr, "replay: --from is required and must be greater than 0")
		fs.Usage()
		return 1
	case *to == 0:
		fmt.Fprintln(os.Stderr, "replay: --to is required and must be greater than 0")
		fs.Usage()
		return 1
	case *to < *from:
		fmt.Fprintf(os.Stderr, "replay: --to (%d) must not be before --from (%d)\n", *to, *from)
		return 1
	case *to > uint64(math.MaxUint32):
		fmt.Fprintf(os.Stderr, "replay: --to (%d) is past the largest ledger sequence\n", *to)
		return 1
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay: invalid configuration: %v\n", err)
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
	if !*dryRun {
		db, err = store.Open(ctx, cfg.DatabaseURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "replay: open database: %v\n", err)
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
	if _, err := replayer.Run(ctx, uint32(*from), uint32(*to), *dryRun); err != nil {
		log.Error("replay failed", "err", err)
		return 1
	}
	return 0
}
