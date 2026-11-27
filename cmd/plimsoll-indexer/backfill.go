package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/config"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/worker"
)

func runBackfillEventTypeCommand(args []string) int {
	fs := flag.NewFlagSet("backfill-event-type", flag.ContinueOnError)
	eventType := fs.String("event-type", "", "Contract event type to backfill (e.g. ThresholdsUpdated)")
	startLedger := fs.Uint("start-ledger", 0, "Start ledger sequence (inclusive)")
	endLedger := fs.Uint("end-ledger", 0, "End ledger sequence (inclusive)")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if *eventType == "" {
		fmt.Fprintln(os.Stderr, "backfill-event-type: --event-type is required")
		return 1
	}
	if *startLedger == 0 {
		fmt.Fprintln(os.Stderr, "backfill-event-type: --start-ledger is required and must be > 0")
		return 1
	}
	if *endLedger > 0 && *endLedger < *startLedger {
		fmt.Fprintln(os.Stderr, "backfill-event-type: --end-ledger must be >= --start-ledger")
		return 1
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "backfill-event-type: configuration error: %v\n", err)
		return 1
	}
	log := newLogger(cfg.LogLevel)

	ctx := context.Background()
	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "backfill-event-type: connect database: %v\n", err)
		return 1
	}
	defer db.Close()

	rpc := rpcclient.NewClient(cfg.RPCURL, nil)
	defer rpc.Close()

	backfiller := &worker.Backfiller{
		RPC:         rpc,
		Store:       db,
		ContractIDs: []string{cfg.CoverageLedgerID, cfg.ReporterRegistryID},
		EventType:   *eventType,
		StartLedger: uint32(*startLedger),
		EndLedger:   uint32(*endLedger),
		Log:         log.With("job", "backfill", "event_type", *eventType),
	}

	if err := backfiller.Run(ctx); err != nil {
		log.Error("backfill-event-type failed", "err", err)
		return 1
	}

	log.Info("backfill-event-type completed successfully", "event_type", *eventType, "start", *startLedger, "end", *endLedger)
	return 0
}
