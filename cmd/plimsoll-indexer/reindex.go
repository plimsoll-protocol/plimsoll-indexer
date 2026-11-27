package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/config"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/worker"
)

// runReindexCommand discards indexed events from --from-ledger onwards, resets
// the ingest cursor and replays them from RPC. With --dry-run it reports the
// row counts without deleting anything.
func runReindexCommand(args []string) int {
	fs := flag.NewFlagSet("reindex", flag.ContinueOnError)
	fromFlag := fs.Uint64("from-ledger", 0, "first ledger to discard and re-ingest")
	dryRun := fs.Bool("dry-run", false, "report what would be deleted without deleting or re-ingesting")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *fromFlag == 0 {
		fmt.Fprintln(os.Stderr, "Error: --from-ledger is required and must be greater than zero")
		fs.Usage()
		return 1
	}
	const maxUint32 = uint64(^uint32(0))
	if *fromFlag > maxUint32 {
		fmt.Fprintf(os.Stderr, "Error: --from-ledger %d is outside the uint32 ledger range\n", *fromFlag)
		return 1
	}
	from := uint32(*fromFlag)

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		return 1
	}
	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("reindex: open store", "err", err)
		return 1
	}
	defer db.Close()

	if *dryRun {
		counts, err := db.CountEventsFromLedger(ctx, from)
		if err != nil {
			log.Error("reindex: count rows", "err", err)
			return 1
		}
		fmt.Fprintf(os.Stdout, "reindex --dry-run from ledger %d would delete:\n", from)
		fmt.Fprintf(os.Stdout, "  assets:           %d\n", counts.Assets)
		fmt.Fprintf(os.Stdout, "  supply_snapshots: %d\n", counts.SupplySnapshots)
		fmt.Fprintf(os.Stdout, "  reserve_reports:  %d\n", counts.ReserveReports)
		fmt.Fprintf(os.Stdout, "  total:            %d\n", counts.Total())
		fmt.Fprintln(os.Stdout, "  ingest cursor:    reset")
		log.Info("reindex dry run", "from_ledger", from,
			"assets", counts.Assets, "supply_snapshots", counts.SupplySnapshots,
			"reserve_reports", counts.ReserveReports, "total", counts.Total())
		return 0
	}

	counts, err := db.DeleteEventsFromLedger(ctx, from)
	if err != nil {
		log.Error("reindex: delete rows", "err", err)
		return 1
	}
	if err := db.ResetIngestCursor(ctx, worker.CursorName); err != nil {
		log.Error("reindex: reset cursor", "err", err)
		return 1
	}
	log.Info("reindex deleted rows",
		"from_ledger", from,
		"assets", counts.Assets,
		"supply_snapshots", counts.SupplySnapshots,
		"reserve_reports", counts.ReserveReports,
		"total", counts.Total())

	rpc := rpcclient.NewClient(cfg.RPCURL, &http.Client{Timeout: 30 * time.Second})
	defer rpc.Close()

	ingester := &worker.Ingester{
		RPC:         rpc,
		Store:       db,
		ContractIDs: []string{cfg.CoverageLedgerID, cfg.ReporterRegistryID},
		StartLedger: from,
		Log:         log.With("job", "reindex"),
	}
	// Report every 1 000 ledgers the scan crosses, starting at the first
	// multiple at or after --from-ledger.
	nextLog := (from/1000 + 1) * 1000
	ingester.OnProgress = func(scanned uint32) {
		if scanned < nextLog {
			return
		}
		log.Info("reindex progress", "scanned_to_ledger", scanned)
		nextLog = (scanned/1000 + 1) * 1000
	}
	log.Info("reindex started", "from_ledger", from)
	if err := ingester.Run(ctx); err != nil {
		log.Error("reindex: ingest", "err", err)
		return 1
	}
	log.Info("reindex complete", "from_ledger", from)
	return 0
}
