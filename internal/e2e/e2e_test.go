// Package e2e runs the indexer's network-facing code against a real local
// Stellar network (stellar/quickstart in Docker). Every test skips unless
// PLIMSOLL_E2E=1, so `go test ./...` never needs Docker:
//
//	PLIMSOLL_E2E=1 go test ./internal/e2e/ -v -timeout 10m
package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/config"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/horizon"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/worker"
)

// quickstartImage can be overridden with PLIMSOLL_E2E_IMAGE.
const quickstartImage = "stellar/quickstart:latest"

// network is a running quickstart container, started once per test binary.
var (
	once    sync.Once
	netErr  error
	cleanup func()
)

func startNetwork(t *testing.T) {
	t.Helper()
	if os.Getenv("PLIMSOLL_E2E") != "1" {
		t.Skip("set PLIMSOLL_E2E=1 to run against a local Stellar network (needs Docker)")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found on PATH")
	}
	once.Do(func() {
		image := os.Getenv("PLIMSOLL_E2E_IMAGE")
		if image == "" {
			image = quickstartImage
		}
		suffix := make([]byte, 4)
		_, _ = rand.Read(suffix)
		name := "plimsoll-e2e-" + hex.EncodeToString(suffix)
		out, err := exec.Command("docker", "run", "-d", "--rm", "--name", name,
			"-p", "8000:8000", image, "--local").CombinedOutput()
		if err != nil {
			netErr = &runError{string(out), err}
			return
		}
		cleanup = func() { _ = exec.Command("docker", "rm", "-f", name).Run() }
	})
	if netErr != nil {
		t.Fatalf("start quickstart: %v", netErr)
	}
}

type runError struct {
	out string
	err error
}

func (e *runError) Error() string { return e.err.Error() + ": " + e.out }

// TestMain removes the container once every test has run.
func TestMain(m *testing.M) {
	code := m.Run()
	if cleanup != nil {
		cleanup()
	}
	os.Exit(code)
}

func waitFor(t *testing.T, what string, timeout time.Duration, ok func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if last = ok(); last == nil {
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("%s not ready after %s: %v", what, timeout, last)
}

// standaloneConfig loads the config the way an operator would: only NETWORK
// is set, so the standalone preset must point at the quickstart container.
func standaloneConfig(t *testing.T) config.Config {
	t.Helper()
	for k, v := range map[string]string{
		"NETWORK":              "standalone",
		"DATABASE_URL":         "postgres://unused",
		"COVERAGE_LEDGER_ID":   "unused",
		"REPORTER_REGISTRY_ID": "unused",
	} {
		t.Setenv(k, v)
	}
	for _, k := range []string{"STELLAR_RPC_URL", "HORIZON_URL", "STELLAR_NETWORK_PASSPHRASE"} {
		t.Setenv(k, "")
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestStandalonePresetReachesLocalNetwork(t *testing.T) {
	startNetwork(t)
	cfg := standaloneConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	hz := horizon.NewClient(cfg.HorizonURL)
	waitFor(t, "horizon", 6*time.Minute, func() error {
		n, err := hz.LatestLedger(ctx)
		if err == nil && n == 0 {
			err = io.ErrNoProgress
		}
		return err
	})

	rpc := rpcclient.NewClient(cfg.RPCURL, &http.Client{Timeout: 30 * time.Second})
	defer rpc.Close()
	waitFor(t, "soroban rpc", 4*time.Minute, func() error {
		_, err := rpc.GetLatestLedger(ctx)
		return err
	})
	latest, err := rpc.GetLatestLedger(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if latest.Sequence == 0 {
		t.Fatal("rpc reports ledger 0")
	}
}

// memStore is the in-memory storage surface the ingester needs.
type memStore struct {
	mu       sync.Mutex
	progress store.Progress
	applied  int
}

func (m *memStore) Progress(context.Context, string) (store.Progress, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.progress, nil
}

func (m *memStore) ApplyEvents(_ context.Context, _, cursor string, ledger uint32, _ []soroban.RawEvent, events []any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.progress.Cursor, m.progress.ScannedLedger = cursor, ledger
	m.applied += len(events)
	return nil
}

func (*memStore) LatestReporterReports(context.Context) ([]store.ReporterLatestReport, error) {
	return nil, nil
}

// The contracts are not deployed to a fresh quickstart network, so this checks
// the ingest loop against real RPC: it must start from the live ledger, page
// without error and advance its cursor with nothing to decode.
func TestIngestAgainstLocalRPC(t *testing.T) {
	startNetwork(t)
	cfg := standaloneConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	rpc := rpcclient.NewClient(cfg.RPCURL, &http.Client{Timeout: 30 * time.Second})
	defer rpc.Close()
	waitFor(t, "soroban rpc", 6*time.Minute, func() error {
		_, err := rpc.GetLatestLedger(ctx)
		return err
	})

	var raw [32]byte
	_, _ = rand.Read(raw[:])
	contract, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	if err != nil {
		t.Fatal(err)
	}

	mem := &memStore{}
	in := &worker.Ingester{
		RPC:         rpc,
		Store:       mem,
		ContractIDs: []string{contract},
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := in.Run(ctx); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if mem.progress.Cursor == "" || mem.progress.ScannedLedger == 0 {
		t.Fatalf("cursor did not advance: %+v", mem.progress)
	}
	if mem.applied != 0 {
		t.Fatalf("decoded %d events for a contract that does not exist", mem.applied)
	}
}
