// Command plimsoll-indexer ingests Plimsoll contract events, posts supply
// snapshots, syncs issuer stellar.toml data, and serves the read API.
package main

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/changelog"
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
	if len(os.Args) > 1 && os.Args[1] == "replay" {
		os.Exit(runReplayCommand(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck())
	}
	if len(os.Args) > 1 && os.Args[1] == "keygen" {
		os.Exit(runKeygenCommand(os.Args[2:]))
	}

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

	// Every RPC call goes through the rate-limit transport, so a node that
	// answers 429 backs the caller off, honouring Retry-After, instead of
	// failing the tick and retrying on the next one.
	rpcHTTP := &http.Client{
		Timeout:   30 * time.Second,
		Transport: soroban.NewRateLimitTransport(nil, log.With("component", "rpc")),
	}
	rpc := rpcclient.NewClient(cfg.RPCURL, rpcHTTP)
	defer rpc.Close()
	hz := horizon.NewClient(cfg.HorizonURL)

	var webhookCodec *webhook.SecretCodec
	var webhookDispatcher *webhook.Dispatcher
	if cfg.WebhookEncryptionKey != "" {
		webhookCodec, err = webhook.NewSecretCodec(cfg.WebhookEncryptionKey)
		if err != nil {
			return err
		}
		webhookDispatcher = webhook.NewDispatcher(db, webhookCodec)
	}

	ingester := &worker.Ingester{
		RPC:         rpc,
		Store:       db,
		ContractIDs: []string{cfg.CoverageLedgerID, cfg.ReporterRegistryID},
		StartLedger: cfg.StartLedger,
		Workers:     cfg.IngestWorkers,
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
		Horizon:     hz,
		Fetcher:     stellartoml.NewFetcher(),
		Store:       db,
		Webhooks:    webhookDispatcher,
		Attestation: stellartoml.NewDocumentFetcher(),
		MaxAge:      cfg.TomlInterval,
		Log:         log.With("job", "toml"),
	}
	// Checks every minute; each asset is refreshed once per TOML_INTERVAL.
	go every(ctx, time.Minute, log.With("job", "toml"), tomlSync.Run)

	if cfg.WebhookSigningSecret != "" {
		notifier := &worker.WebhookNotifier{
			Store:  db,
			Secret: cfg.WebhookSigningSecret,
			Log:    log.With("job", "webhooks"),
			Now:    time.Now,
		}
		go every(ctx, cfg.WebhookInterval, log.With("job", "webhooks"), notifier.Run)
	}

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
			Horizon:      hz,
			RPC:          rpc,
			Invoker:      invoker,
			Store:        db,
			LedgerID:     cfg.CoverageLedgerID,
			RepostAfter:  cfg.RepostAfter,
			DecimalCheck: cfg.DecimalCheckEnabled,
			Log:          log.With("job", "poster"),
			Now:          time.Now,
		}
		// Left nil when no second Horizon is configured: comparing hz against
		// itself would always agree and prove nothing.
		if cfg.HorizonCheckURL != "" {
			poster.HorizonCheck = horizon.NewClient(cfg.HorizonCheckURL)
			log.Info("supply cross-check enabled", "check_horizon", cfg.HorizonCheckURL)
		} else {
			log.Info("supply cross-check disabled (HORIZON_CHECK_URL not set)")
		}
		log.Info("supply posting enabled", "poster", signer.Address())
		go every(ctx, cfg.PostInterval, log.With("job", "poster"), poster.Run)

	} else {
		log.Info("supply posting disabled (SUPPLY_POSTER_SECRET not set)")
	}

	// A malformed changelog would break the published API contract, so refuse
	// to start rather than serve it.
	changes, err := changelog.Load()
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr: ":" + cfg.Port,
		Handler: (&api.Server{
			Store:             db,
			WebhookStore:      db,
			WebhookAdminToken: cfg.WebhookAdminToken,
			WebhookAPIKeys:    cfg.WebhookAPIKeys,
			WebhookCodec:      webhookCodec,
			Network: api.NetworkInfo{
				NetworkPassphrase:  cfg.NetworkPassphrase,
				RPCURL:             cfg.RPCURL,
				HorizonURL:         cfg.HorizonURL,
				CoverageLedgerID:   cfg.CoverageLedgerID,
				ReporterRegistryID: cfg.ReporterRegistryID,
			},
			CoverageWebhooks:     db,
			WebhookSigningSecret: cfg.WebhookSigningSecret,
			CORS:                 cfg.CORSOrigins,
			Log:                  log.With("component", "api"),
			Now:                  time.Now,
			Changelog:            changes,
		}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second, // Bounds the time clients can spend sending request bodies.
		WriteTimeout:      30 * time.Second, // Closes connections when clients stall while receiving responses.
		IdleTimeout:       60 * time.Second, // Reclaims keep-alive connections that are left idle.
		MaxHeaderBytes:    1 << 20,          // Caps request headers at 1 MiB to limit per-connection memory use.
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
