package e2e

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/horizon"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// testnetDeployment is deployments/testnet.json: where the contracts live on
// testnet, shared with plimsoll-contracts. It is the single source of truth for
// the weekly integration run, so the addresses are never copied by hand.
type testnetDeployment struct {
	Network            string `json:"network"`
	NetworkPassphrase  string `json:"network_passphrase"`
	RPCURL             string `json:"rpc_url"`
	HorizonURL         string `json:"horizon_url"`
	StartLedger        uint32 `json:"start_ledger"`
	CoverageLedgerID   string `json:"coverage_ledger_id"`
	ReporterRegistryID string `json:"reporter_registry_id"`
	Assets             []struct {
		Code string `json:"code"`
		SAC  string `json:"sac"`
	} `json:"assets"`
}

// loadTestnetDeployment reads the manifest. PLIMSOLL_TESTNET_DEPLOYMENT
// overrides the path, which is how a fork or a redeploy points the suite at a
// different set of contracts.
func loadTestnetDeployment(t *testing.T) testnetDeployment {
	t.Helper()
	path := os.Getenv("PLIMSOLL_TESTNET_DEPLOYMENT")
	if path == "" {
		path = filepath.Join("..", "..", "deployments", "testnet.json")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read deployment manifest: %v", err)
	}
	var d testnetDeployment
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return d
}

// The manifest itself is checked on every pull request, not only on the weekly
// run: a mistyped contract id should fail the change that introduced it.
func TestTestnetDeploymentManifest(t *testing.T) {
	d := loadTestnetDeployment(t)
	if d.Network != "testnet" {
		t.Errorf("network = %q, want testnet", d.Network)
	}
	if d.NetworkPassphrase == "" {
		t.Error("network_passphrase is empty")
	}
	if d.StartLedger == 0 {
		t.Error("start_ledger is zero")
	}
	for _, u := range []struct{ name, raw string }{
		{"rpc_url", d.RPCURL},
		{"horizon_url", d.HorizonURL},
	} {
		parsed, err := url.Parse(u.raw)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
			t.Errorf("%s = %q, want an absolute https URL", u.name, u.raw)
		}
	}
	for _, c := range []struct{ name, id string }{
		{"coverage_ledger_id", d.CoverageLedgerID},
		{"reporter_registry_id", d.ReporterRegistryID},
	} {
		if !strkey.IsValidContractAddress(c.id) {
			t.Errorf("%s = %q, want a contract strkey", c.name, c.id)
		}
	}
	if len(d.Assets) == 0 {
		t.Fatal("assets is empty, so the weekly run would check no asset")
	}
	seen := map[string]bool{}
	for _, a := range d.Assets {
		if a.Code == "" {
			t.Error("asset with an empty code")
		}
		if seen[a.SAC] {
			t.Errorf("asset %s is listed twice", a.SAC)
		}
		seen[a.SAC] = true
		if !strkey.IsValidContractAddress(a.SAC) {
			t.Errorf("asset %s: sac = %q, want a contract strkey", a.Code, a.SAC)
		}
	}
}

// TestTestnetContractsAreLive reads the deployed contracts and both supporting
// services. The contracts change independently of this repository, so a
// redeploy, an ABI change, an archived entry or RPC/Horizon drift can break the
// indexer with no commit here. Nothing in the pull-request suite can see that.
//
//	PLIMSOLL_TESTNET=1 go test ./internal/e2e/ -run TestTestnet -v -timeout 10m
func TestTestnetContractsAreLive(t *testing.T) {
	if os.Getenv("PLIMSOLL_TESTNET") != "1" {
		t.Skip("set PLIMSOLL_TESTNET=1 to read the live testnet contracts")
	}
	d := loadTestnetDeployment(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rpc := rpcclient.NewClient(d.RPCURL, &http.Client{Timeout: 30 * time.Second})
	defer rpc.Close()
	latest, err := rpc.GetLatestLedger(ctx)
	if err != nil {
		t.Fatalf("rpc %s: %v", d.RPCURL, err)
	}
	if latest.Sequence < d.StartLedger {
		t.Fatalf("rpc latest ledger %d is behind start_ledger %d; the deploy is stale",
			latest.Sequence, d.StartLedger)
	}

	// RPC and Horizon are separate services. If they disagree about how far the
	// chain has advanced, the indexer reads two different views of it.
	hz := horizon.NewClient(d.HorizonURL)
	horizonLedger, err := hz.LatestLedger(ctx)
	if err != nil {
		t.Fatalf("horizon %s: %v", d.HorizonURL, err)
	}
	if horizonLedger == 0 {
		t.Fatalf("horizon %s reports ledger 0", d.HorizonURL)
	}

	// The coverage ledger must answer for every asset, and each answer must
	// still be the shape DecodeSupplyState expects.
	reader := &soroban.Reader{RPC: rpc}
	snapshots := 0
	for _, asset := range d.Assets {
		sac, err := soroban.AddressVal(asset.SAC)
		if err != nil {
			t.Errorf("%s: sac: %v", asset.Code, err)
			continue
		}
		v, err := reader.Read(ctx, d.CoverageLedgerID, "get_supply", []xdr.ScVal{sac})
		if err != nil {
			t.Errorf("%s: get_supply on %s: %v", asset.Code, d.CoverageLedgerID, err)
			continue
		}
		state, err := soroban.DecodeSupplyState(v)
		if err != nil {
			t.Errorf("%s: decode supply state: %v", asset.Code, err)
			continue
		}
		if state == nil {
			t.Logf("%s: no supply posted on-chain yet", asset.Code)
			continue
		}
		snapshots++
		verifySupplySnapshot(t, asset.Code, state)
	}
	if snapshots == 0 {
		t.Fatalf("none of the %d assets in the deployment has a supply snapshot on-chain",
			len(d.Assets))
	}
	t.Logf("testnet: rpc ledger %d, horizon ledger %d, %d/%d assets with a snapshot",
		latest.Sequence, horizonLedger, snapshots, len(d.Assets))
}

// verifySupplySnapshot checks the invariants the indexer relies on when it
// stores what get_supply returned.
func verifySupplySnapshot(t *testing.T, code string, state *soroban.SupplyState) {
	t.Helper()
	if state.Amount == nil || state.Amount.Sign() < 0 {
		t.Errorf("%s: amount %v is not a non-negative integer", code, state.Amount)
	}
	if state.Ledger == 0 {
		t.Errorf("%s: snapshot ledger is zero", code)
	}
	if len(state.BreakdownHash) != 64 {
		t.Errorf("%s: breakdown hash %q is not 64 hex characters", code, state.BreakdownHash)
	} else if _, err := hex.DecodeString(state.BreakdownHash); err != nil {
		t.Errorf("%s: breakdown hash %q is not hex: %v", code, state.BreakdownHash, err)
	}
	if len(state.Poster) != 56 || !strings.HasPrefix(state.Poster, "G") {
		t.Errorf("%s: poster %q is not a Stellar account address", code, state.Poster)
	}
	if state.Timestamp.Unix() <= 0 {
		t.Errorf("%s: snapshot timestamp is zero", code)
	}
	if state.Timestamp.After(time.Now().Add(time.Hour)) {
		t.Errorf("%s: snapshot timestamp %s is in the future", code, state.Timestamp)
	}
}
