// Command plimsoll-indexer ingests Plimsoll contract events, posts supply
// snapshots, syncs issuer stellar.toml data, and serves the read API.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/webhook"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/worker"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "verify" {
		os.Exit(runVerifyCommand(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck())
	}

	cfg, err := config.Load()
	loggers := newComponentLoggers(os.Stdout, config.LogLevels{
		Default: cfg.LogLevel,
		API:     cfg.LogLevelAPI,
		Worker:  cfg.LogLevelWorker,
		Ingest:  cfg.LogLevelIngest,
	})
	if err != nil {
		loggers.base.Error("invalid configuration", "err", err)
		os.Exit(1)
	}
	if err := run(cfg, loggers); err != nil {
		loggers.base.Error("indexer stopped", "err", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, loggers *componentLoggers) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stopReload := watchLogLevelReload(ctx, loggers, config.LoadLogLevels)
	defer stopReload()

	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	rpc := rpcclient.NewClient(cfg.RPCURL, &http.Client{Timeout: 30 * time.Second})
	defer rpc.Close()
	hz := horizon.NewClient(cfg.HorizonURL)

	var webhookCodec *webhook.SecretCodec
	var webhookDispatcher *webhook.Dispatcher
	if cfg.WebhookAdminToken != "" {
		webhookCodec, err = webhook.NewSecretCodec(cfg.WebhookEncryptionKey)
		if err != nil {
			return err
		}
		webhookDispatcher = webhook.NewDispatcher(db, webhookCodec)
	}

	ingestLog := loggers.ingest.With("job", "ingest")
	ingester := &worker.Ingester{
		RPC:         rpc,
		Store:       db,
		ContractIDs: []string{cfg.CoverageLedgerID, cfg.ReporterRegistryID},
		StartLedger: cfg.StartLedger,
		Log:         ingestLog,
	}
	// Ingest once before the other jobs start, so they see the listed assets.
	if err := ingester.Run(ctx); err != nil {
		ingestLog.Error("initial ingest failed", "err", err)
	}
	go every(ctx, cfg.IngestInterval, ingestLog, ingester.Run)

	// Repairs what an ingestion gap lost by reading contract state back.
	// Reading is simulation, so this needs no key and runs even on an instance
	// that cannot sign.
	reconcileLog := loggers.worker.With("job", "reconcile")
	reconciler := &worker.Reconciler{
		Invoker:  &soroban.Reader{RPC: rpc},
		Store:    db,
		LedgerID: cfg.CoverageLedgerID,
		Log:      reconcileLog,
	}
	if err := reconciler.Run(ctx); err != nil {
		reconcileLog.Error("initial reconcile failed", "err", err)
	}
	go every(ctx, cfg.ReconcileInterval, reconcileLog, reconciler.Run)

	tomlLog := loggers.worker.With("job", "toml")
	tomlSync := &worker.TomlSync{
		Horizon:  hz,
		Fetcher:  stellartoml.NewFetcher(),
		Store:    db,
		Webhooks: webhookDispatcher,
		MaxAge:   cfg.TomlInterval,
		Log:      tomlLog,
	}
	// Checks every minute; each asset is refreshed once per TOML_INTERVAL.
	go every(ctx, time.Minute, tomlLog, tomlSync.Run)

	if cfg.KeepAwakeURL != "" {
		keepAwake := worker.NewKeepAwake(cfg.KeepAwakeURL)
		kaLog := loggers.worker.With("job", "keepawake")
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
		posterLog := loggers.worker.With("job", "poster")
		poster := &worker.Poster{
			Horizon:     hz,
			RPC:         rpc,
			Invoker:     invoker,
			Store:       db,
			LedgerID:    cfg.CoverageLedgerID,
			RepostAfter: cfg.RepostAfter,
			Log:         posterLog,
			Now:         time.Now,
		}
		// Left nil when no second Horizon is configured: comparing hz against
		// itself would always agree and prove nothing.
		if cfg.HorizonCheckURL != "" {
			poster.HorizonCheck = horizon.NewClient(cfg.HorizonCheckURL)
			posterLog.Info("supply cross-check enabled", "check_horizon", cfg.HorizonCheckURL)
		} else {
			posterLog.Info("supply cross-check disabled (HORIZON_CHECK_URL not set)")
		}
		posterLog.Info("supply posting enabled", "poster", signer.Address())
		go every(ctx, cfg.PostInterval, posterLog, poster.Run)

	} else {
		loggers.worker.Info("supply posting disabled (SUPPLY_POSTER_SECRET not set)")
	}

	srv := &http.Server{
		Addr: ":" + cfg.Port,
		Handler: (&api.Server{
			Store:             db,
			WebhookStore:      db,
			WebhookAdminToken: cfg.WebhookAdminToken,
			WebhookCodec:      webhookCodec,
			Network: api.NetworkInfo{
				NetworkPassphrase:  cfg.NetworkPassphrase,
				RPCURL:             cfg.RPCURL,
				HorizonURL:         cfg.HorizonURL,
				CoverageLedgerID:   cfg.CoverageLedgerID,
				ReporterRegistryID: cfg.ReporterRegistryID,
			},
			CORS: cfg.CORSOrigins,
			Log:  loggers.api.With("component", "api"),
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
	loggers.api.Info("listening", "addr", srv.Addr)
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

// runHealthcheck requests /healthz on the local server and exits 0 on success
// or 1 on any failure. It honours the PORT environment variable so it matches
// whatever port the container is actually listening on.
func runHealthcheck() int {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+port+"/healthz", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return 0
	}
	fmt.Fprintf(os.Stderr, "healthcheck: status %d\n", resp.StatusCode)
	return 1
}

type componentLoggers struct {
	base        *slog.Logger
	api         *slog.Logger
	worker      *slog.Logger
	ingest      *slog.Logger
	baseLevel   *slog.LevelVar
	apiLevel    *slog.LevelVar
	workerLevel *slog.LevelVar
	ingestLevel *slog.LevelVar
}

func newComponentLoggers(out io.Writer, levels config.LogLevels) *componentLoggers {
	c := &componentLoggers{
		baseLevel:   new(slog.LevelVar),
		apiLevel:    new(slog.LevelVar),
		workerLevel: new(slog.LevelVar),
		ingestLevel: new(slog.LevelVar),
	}
	c.base = slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: c.baseLevel}))
	c.api = slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: c.apiLevel}))
	c.worker = slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: c.workerLevel}))
	c.ingest = slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: c.ingestLevel}))
	c.apply(levels)
	return c
}

func (c *componentLoggers) apply(levels config.LogLevels) {
	c.baseLevel.Set(config.ParseLogLevel(levels.Default))
	c.apiLevel.Set(config.ParseLogLevel(levels.API))
	c.workerLevel.Set(config.ParseLogLevel(levels.Worker))
	c.ingestLevel.Set(config.ParseLogLevel(levels.Ingest))
}

func watchLogLevelReload(ctx context.Context, loggers *componentLoggers, reload func() config.LogLevels) func() {
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-quit:
				return
			case <-sighup:
				levels := reload()
				loggers.apply(levels)
				loggers.base.Info("reloaded log levels",
					"default", levels.Default,
					"api", levels.API,
					"worker", levels.Worker,
					"ingest", levels.Ingest)
			}
		}
	}()
	return func() {
		signal.Stop(sighup)
		close(quit)
		<-done
	}
}
