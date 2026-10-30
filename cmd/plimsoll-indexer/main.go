// Command plimsoll-indexer ingests Plimsoll contract events, posts supply
// snapshots, syncs issuer stellar.toml data, and serves the read API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/keypair"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/api"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/config"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/horizon"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/stellartoml"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/worker"
)

func main() {
	cfg, err := config.Load()
	log := newLogger(cfg.LogLevel)
	if err != nil {
		log.Error("invalid configuration", "err", err)
		os.Exit(1)
	}
	if err := run(cfg, log); err != nil {
		log.Error("indexer stopped", "err", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	rpc := rpcclient.NewClient(cfg.RPCURL, &http.Client{Timeout: 30 * time.Second})
	defer rpc.Close()
	hz := horizon.NewClient(cfg.HorizonURL)

	ingester := &worker.Ingester{
		RPC:         rpc,
		Store:       db,
		ContractIDs: []string{cfg.CoverageLedgerID, cfg.ReporterRegistryID},
		StartLedger: cfg.StartLedger,
		Log:         log.With("job", "ingest"),
	}
	// Ingest once before the other jobs start, so they see the listed assets.
	if err := ingester.Run(ctx); err != nil {
		log.Error("initial ingest failed", "err", err)
	}
	go every(ctx, cfg.IngestInterval, log.With("job", "ingest"), ingester.Run)

	// Repairs what an ingestion gap lost by reading contract state back.
	// Reading is simulation, so this needs no key and runs even on an instance
	// that cannot sign.
	reconciler := &worker.Reconciler{
		Invoker:  &soroban.Reader{RPC: rpc},
		Store:    db,
		LedgerID: cfg.CoverageLedgerID,
		Log:      log.With("job", "reconcile"),
	}
	if err := reconciler.Run(ctx); err != nil {
		log.Error("initial reconcile failed", "err", err)
	}
	go every(ctx, cfg.ReconcileInterval, log.With("job", "reconcile"), reconciler.Run)

	tomlSync := &worker.TomlSync{
		Horizon: hz,
		Fetcher: stellartoml.NewFetcher(),
		Store:   db,
		MaxAge:  cfg.TomlInterval,
		Log:     log.With("job", "toml"),
	}
	// Checks every minute; each asset is refreshed once per TOML_INTERVAL.
	go every(ctx, time.Minute, log.With("job", "toml"), tomlSync.Run)

	if cfg.KeepAwakeURL != "" {
		keepAwake := worker.NewKeepAwake(cfg.KeepAwakeURL)
		kaLog := log.With("job", "keepawake")
		kaLog.Info("keep-awake enabled", "url", cfg.KeepAwakeURL, "interval", cfg.KeepAwakeInterval)
		go func() {
			// Wait one interval first: the HTTP server is not listening yet.
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.KeepAwakeInterval):
			}
			every(ctx, cfg.KeepAwakeInterval, kaLog, keepAwake.Run)
		}()
	}

	if cfg.PostingEnabled() {
		signer, err := keypair.ParseFull(cfg.PosterSecret)
		if err != nil {
			return errors.New("SUPPLY_POSTER_SECRET is not a valid secret seed")
		}
		invoker := &soroban.Invoker{
			RPC:        rpc,
			Signer:     signer,
			Passphrase: cfg.NetworkPassphrase,
		}
		poster := &worker.Poster{
			Horizon:     hz,
			RPC:         rpc,
			Invoker:     invoker,
			Store:       db,
			LedgerID:    cfg.CoverageLedgerID,
			RepostAfter: cfg.RepostAfter,
			Log:         log.With("job", "poster"),
			Now:         time.Now,
		}
		log.Info("supply posting enabled", "poster", signer.Address())
		go every(ctx, cfg.PostInterval, log.With("job", "poster"), poster.Run)

	} else {
		log.Info("supply posting disabled (SUPPLY_POSTER_SECRET not set)")
	}

	srv := &http.Server{
		Addr: ":" + cfg.Port,
		Handler: (&api.Server{
			Store: db,
			Network: api.NetworkInfo{
				NetworkPassphrase:  cfg.NetworkPassphrase,
				RPCURL:             cfg.RPCURL,
				HorizonURL:         cfg.HorizonURL,
				CoverageLedgerID:   cfg.CoverageLedgerID,
				ReporterRegistryID: cfg.ReporterRegistryID,
			},
			CORS: cfg.CORSOrigins,
			Log:  log.With("component", "api"),
			Now:  time.Now,
		}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// every runs fn immediately, then on each tick, until ctx is done.
func every(ctx context.Context, interval time.Duration, log *slog.Logger, fn func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			log.Error("job failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
