package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/horizon"
)

func TestPrintVerificationResult(t *testing.T) {
	snap := horizon.Breakdown{
		Asset:                           "PUSD:GISS",
		Ledger:                          100,
		Authorized:                      "10000000000",
		AuthorizedToMaintainLiabilities: "0",
		Unauthorized:                    "0",
		ClaimableBalances:               "0",
		LiquidityPools:                  "0",
		Contracts:                       "0",
		Total:                           "10000000000",
	}

	curMatch := snap
	curMatch.Ledger = 105

	curMismatch := snap
	curMismatch.Ledger = 105
	curMismatch.Authorized = "12000000000"
	curMismatch.Total = "12000000000"

	t.Run("match with differing ledgers prints note", func(t *testing.T) {
		var out bytes.Buffer
		code := printVerificationResult(&out, "PUSD", "CSAC", snap, curMatch)
		if code != 0 {
			t.Fatalf("expected code 0, got %d", code)
		}
		str := out.String()
		if !strings.Contains(str, "Result: MATCH") {
			t.Fatalf("expected MATCH in output, got:\n%s", str)
		}
		if !strings.Contains(str, "Note: Snapshot ledger (100) and current Horizon ledger (105) differ") {
			t.Fatalf("expected ledger difference note in output, got:\n%s", str)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		var out bytes.Buffer
		code := printVerificationResult(&out, "PUSD", "CSAC", snap, curMismatch)
		if code != 1 {
			t.Fatalf("expected code 1, got %d", code)
		}
		str := out.String()
		if !strings.Contains(str, "Result: MISMATCH") {
			t.Fatalf("expected MISMATCH in output, got:\n%s", str)
		}
		if !strings.Contains(str, "+2000000000") {
			t.Fatalf("expected +2000000000 diff, got:\n%s", str)
		}
	})
}

func TestRunVerifyCommand(t *testing.T) {
	bd := horizon.Breakdown{
		Asset:                           "PUSD:GISS",
		Ledger:                          100,
		Authorized:                      "1000",
		AuthorizedToMaintainLiabilities: "0",
		Unauthorized:                    "0",
		ClaimableBalances:               "0",
		LiquidityPools:                  "0",
		Contracts:                       "0",
		Total:                           "1000",
	}
	bdBytes := bd.Canonical()
	bdHash := bd.Hash()

	indexerMux := http.NewServeMux()
	indexerMux.HandleFunc("GET /v1/assets/CSAC", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(apiAssetResp{
			SAC:    "CSAC",
			Code:   "PUSD",
			Issuer: "GISS",
			Supply: &struct {
				Amount        string `json:"amount"`
				Ledger        uint32 `json:"ledger"`
				BreakdownHash string `json:"breakdown_hash"`
			}{
				Amount:        "1000",
				Ledger:        100,
				BreakdownHash: bdHash,
			},
		})
	})
	indexerMux.HandleFunc("GET /v1/breakdowns/"+bdHash, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bdBytes)
	})

	indexerSrv := httptest.NewServer(indexerMux)
	defer indexerSrv.Close()

	horizonMux := http.NewServeMux()
	horizonMux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"history_latest_ledger":105}`))
	})
	horizonMux.HandleFunc("GET /assets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := `{"_embedded":{"records":[{"balances":{"authorized":"0.0001000","authorized_to_maintain_liabilities":"0","unauthorized":"0"},"claimable_balances_amount":"0","liquidity_pools_amount":"0","contracts_amount":"0"}]}}`
		_, _ = w.Write([]byte(resp))
	})

	horizonSrv := httptest.NewServer(horizonMux)
	defer horizonSrv.Close()

	// Mock on-chain RPC fetcher to match indexer's breakdown hash and ledger
	origFetcher := onChainSupplyFetcher
	defer func() { onChainSupplyFetcher = origFetcher }()
	onChainSupplyFetcher = func(ctx context.Context, rpcURL, ledgerID, sac string) (string, uint32, error) {
		return bdHash, 100, nil
	}

	t.Run("missing sac flag", func(t *testing.T) {
		code := runVerifyCommand([]string{})
		if code != 1 {
			t.Fatalf("expected code 1, got %d", code)
		}
	})

	t.Run("missing horizon URL error when discovery fails", func(t *testing.T) {
		code := runVerifyCommand([]string{
			"--sac", "CSAC",
			"--indexer", indexerSrv.URL,
		})
		if code != 1 {
			t.Fatalf("expected code 1 when horizon URL cannot be discovered, got %d", code)
		}
	})

	t.Run("successful verify match with on-chain check", func(t *testing.T) {
		code := runVerifyCommand([]string{
			"--sac", "CSAC",
			"--indexer", indexerSrv.URL,
			"--horizon", horizonSrv.URL,
			"--rpc", "https://mock.rpc",
			"--ledger-id", "CLEDGER",
		})
		if code != 0 {
			t.Fatalf("expected verify code 0, got %d", code)
		}
	})
}
