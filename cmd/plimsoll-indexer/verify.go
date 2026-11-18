package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/horizon"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

type apiAssetResp struct {
	SAC    string `json:"sac"`
	Code   string `json:"code"`
	Issuer string `json:"issuer"`
	Supply *struct {
		Amount        string `json:"amount"`
		Ledger        uint32 `json:"ledger"`
		BreakdownHash string `json:"breakdown_hash"`
	} `json:"supply"`
}

type apiNetworkResp struct {
	RPCURL             string `json:"rpc_url"`
	HorizonURL         string `json:"horizon_url"`
	CoverageLedgerID   string `json:"coverage_ledger_id"`
	ReporterRegistryID string `json:"reporter_registry_id"`
}

// onChainSupplyFetcher allows mocking on-chain RPC calls in unit tests.
var onChainSupplyFetcher = fetchOnChainSupply

func runVerifyCommand(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	sacFlag := fs.String("sac", "", "Stellar Asset Contract (SAC) address")
	indexerFlag := fs.String("indexer", "http://localhost:8080", "Indexer API base URL")
	horizonFlag := fs.String("horizon", "", "Horizon API base URL (optional)")
	rpcFlag := fs.String("rpc", "", "Soroban RPC URL (optional)")
	ledgerIDFlag := fs.String("ledger-id", "", "Coverage ledger contract ID (optional)")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if *sacFlag == "" {
		fmt.Fprintln(os.Stderr, "Error: --sac flag is required")
		fs.Usage()
		return 1
	}

	indexerURL := strings.TrimRight(*indexerFlag, "/")
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	httpClient := &http.Client{Timeout: 15 * time.Second}

	// 1. Discover network parameters if needed
	horizonURL := *horizonFlag
	rpcURL := *rpcFlag
	ledgerID := *ledgerIDFlag

	if horizonURL == "" || rpcURL == "" || ledgerID == "" {
		netReq, err := http.NewRequestWithContext(ctx, http.MethodGet, indexerURL+"/v1/network", nil)
		if err == nil {
			if netResp, err := httpClient.Do(netReq); err == nil {
				if netResp.StatusCode == http.StatusOK {
					var netInfo apiNetworkResp
					if json.NewDecoder(netResp.Body).Decode(&netInfo) == nil {
						if horizonURL == "" {
							horizonURL = netInfo.HorizonURL
						}
						if rpcURL == "" {
							rpcURL = netInfo.RPCURL
						}
						if ledgerID == "" {
							ledgerID = netInfo.CoverageLedgerID
						}
					}
				}
				netResp.Body.Close()
			}
		}
	}

	if horizonURL == "" {
		fmt.Fprintln(os.Stderr, "Error: could not discover Horizon URL from indexer; please specify --horizon <URL>")
		return 1
	}

	// 2. Fetch asset details from indexer
	assetReq, err := http.NewRequestWithContext(ctx, http.MethodGet, indexerURL+"/v1/assets/"+*sacFlag, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating request: %v\n", err)
		return 1
	}
	assetResp, err := httpClient.Do(assetReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error fetching asset from indexer: %v\n", err)
		return 1
	}
	defer assetResp.Body.Close()

	if assetResp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Error: indexer returned status %d for asset %s\n", assetResp.StatusCode, *sacFlag)
		return 1
	}

	var asset apiAssetResp
	if err := json.NewDecoder(assetResp.Body).Decode(&asset); err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding asset response: %v\n", err)
		return 1
	}

	if asset.Supply == nil || asset.Supply.BreakdownHash == "" {
		fmt.Fprintf(os.Stderr, "Error: no supply snapshot found for asset %s\n", *sacFlag)
		return 1
	}

	// 3. Verify on-chain snapshot via RPC simulation
	if rpcURL != "" && ledgerID != "" {
		onChainHash, onChainLedger, err := onChainSupplyFetcher(ctx, rpcURL, ledgerID, *sacFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error verifying on-chain snapshot: %v\n", err)
			return 1
		}

		if onChainHash != asset.Supply.BreakdownHash || onChainLedger != asset.Supply.Ledger {
			fmt.Fprintf(os.Stderr, "Error: on-chain supply mismatch with indexer! On-chain breakdown_hash=%s, ledger=%d; indexer breakdown_hash=%s, ledger=%d\n",
				onChainHash, onChainLedger, asset.Supply.BreakdownHash, asset.Supply.Ledger)
			return 1
		}
	} else {
		fmt.Fprintln(os.Stderr, "Warning: could not discover RPC URL or coverage_ledger_id to perform on-chain simulation verification")
	}

	// 4. Fetch breakdown from indexer
	breakdownReq, err := http.NewRequestWithContext(ctx, http.MethodGet, indexerURL+"/v1/breakdowns/"+asset.Supply.BreakdownHash, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating breakdown request: %v\n", err)
		return 1
	}
	breakdownResp, err := httpClient.Do(breakdownReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error fetching breakdown from indexer: %v\n", err)
		return 1
	}
	defer breakdownResp.Body.Close()

	if breakdownResp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Error: indexer returned status %d for breakdown %s\n", breakdownResp.StatusCode, asset.Supply.BreakdownHash)
		return 1
	}

	breakdownBytes, err := io.ReadAll(breakdownResp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading breakdown body: %v\n", err)
		return 1
	}

	// Verify SHA-256 hash match against body
	computedHash := sha256.Sum256(breakdownBytes)
	computedHashHex := hex.EncodeToString(computedHash[:])
	if computedHashHex != asset.Supply.BreakdownHash {
		fmt.Fprintf(os.Stderr, "Error: breakdown hash mismatch! Expected %s, got %s\n", asset.Supply.BreakdownHash, computedHashHex)
		return 1
	}

	var snapshotBreakdown horizon.Breakdown
	if err := json.Unmarshal(breakdownBytes, &snapshotBreakdown); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing breakdown JSON: %v\n", err)
		return 1
	}

	// 5. Fetch current supply from Horizon
	hzClient := horizon.NewClient(horizonURL)
	curBreakdown, err := hzClient.Supply(ctx, asset.Code, asset.Issuer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error fetching supply from Horizon (%s): %v\n", horizonURL, err)
		return 1
	}

	// 6. Compare & print diff table
	return printVerificationResult(os.Stdout, asset.Code, asset.SAC, snapshotBreakdown, curBreakdown)
}

func fetchOnChainSupply(ctx context.Context, rpcURL, ledgerID, sac string) (string, uint32, error) {
	if rpcURL == "" || ledgerID == "" {
		return "", 0, errors.New("rpc URL and coverage ledger contract ID are required")
	}
	rpcClient := rpcclient.NewClient(rpcURL, &http.Client{Timeout: 15 * time.Second})

	contractAddr, err := soroban.ScAddress(ledgerID)
	if err != nil {
		return "", 0, fmt.Errorf("invalid coverage_ledger_id %q: %w", ledgerID, err)
	}
	sacVal, err := soroban.AddressVal(sac)
	if err != nil {
		return "", 0, fmt.Errorf("invalid sac %q: %w", sac, err)
	}

	hostFn := xdr.HostFunction{
		Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
		InvokeContract: &xdr.InvokeContractArgs{
			ContractAddress: contractAddr,
			FunctionName:    xdr.ScSymbol("get_supply"),
			Args:            xdr.ScVec([]xdr.ScVal{sacVal}),
		},
	}

	dummySource := "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"
	sourceAccount := txnbuild.NewSimpleAccount(dummySource, 0)
	op := &txnbuild.InvokeHostFunction{HostFunction: hostFn, SourceAccount: dummySource}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &sourceAccount,
		IncrementSequenceNum: false,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
	})
	if err != nil {
		return "", 0, fmt.Errorf("build simulation tx: %w", err)
	}

	simB64, err := tx.Base64()
	if err != nil {
		return "", 0, fmt.Errorf("encode tx base64: %w", err)
	}

	sim, err := rpcClient.SimulateTransaction(ctx, protocol.SimulateTransactionRequest{Transaction: simB64})
	if err != nil {
		return "", 0, fmt.Errorf("rpc simulate get_supply: %w", err)
	}
	if sim.Error != "" {
		return "", 0, fmt.Errorf("on-chain simulation error: %s", sim.Error)
	}
	if len(sim.Results) == 0 {
		return "", 0, errors.New("on-chain simulation returned empty results")
	}

	if sim.Results[0].ReturnValueXDR == nil {
		return "", 0, errors.New("on-chain simulation return value is nil")
	}
	var resVal xdr.ScVal
	if err := xdr.SafeUnmarshalBase64(*sim.Results[0].ReturnValueXDR, &resVal); err != nil {
		return "", 0, fmt.Errorf("decode result scval: %w", err)
	}

	var val xdr.ScVal
	switch resVal.Type {
	case xdr.ScValTypeScvVoid:
		return "", 0, errors.New("no supply snapshot posted on-chain for asset")
	case xdr.ScValTypeScvVec:
		if resVal.Vec == nil || *resVal.Vec == nil {
			return "", 0, errors.New("no supply snapshot posted on-chain for asset")
		}
		vec := **resVal.Vec
		if len(vec) == 0 {
			return "", 0, errors.New("no supply snapshot posted on-chain for asset")
		}
		val = vec[0]
	default:
		val = resVal
	}

	fields, err := soroban.Fields(val)
	if err != nil {
		return "", 0, fmt.Errorf("parse on-chain supply fields: %w", err)
	}

	hashVal, ok := fields["breakdown_hash"]
	if !ok {
		return "", 0, errors.New("on-chain supply missing breakdown_hash field")
	}
	breakdownHash, err := soroban.AsHex(hashVal)
	if err != nil {
		return "", 0, fmt.Errorf("parse on-chain breakdown_hash: %w", err)
	}

	ledgerVal, ok := fields["ledger"]
	if !ok {
		return "", 0, errors.New("on-chain supply missing ledger field")
	}
	ledger, err := soroban.AsU32(ledgerVal)
	if err != nil {
		return "", 0, fmt.Errorf("parse on-chain ledger: %w", err)
	}

	return breakdownHash, ledger, nil
}

func printVerificationResult(out io.Writer, code, sac string, snapshot, current horizon.Breakdown) int {
	fmt.Fprintf(out, "Asset: %s (SAC: %s)\n", code, sac)
	fmt.Fprintf(out, "Snapshot Ledger: %d | Current Horizon Ledger: %d\n", snapshot.Ledger, current.Ledger)
	fmt.Fprintf(out, "Breakdown Hash: %s (SHA-256 match: OK)\n", snapshot.Hash())

	if snapshot.Ledger != current.Ledger {
		fmt.Fprintf(out, "Note: Snapshot ledger (%d) and current Horizon ledger (%d) differ; a mismatch may indicate supply moved since snapshot.\n", snapshot.Ledger, current.Ledger)
	}
	fmt.Fprintln(out)

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Bucket\tSnapshot\tCurrent Horizon\tDiff")
	fmt.Fprintln(w, "------\t--------\t---------------\t----")

	buckets := []struct {
		name string
		snap string
		cur  string
	}{
		{"Authorized", snapshot.Authorized, current.Authorized},
		{"AuthorizedToMaintainLiabilities", snapshot.AuthorizedToMaintainLiabilities, current.AuthorizedToMaintainLiabilities},
		{"Unauthorized", snapshot.Unauthorized, current.Unauthorized},
		{"ClaimableBalances", snapshot.ClaimableBalances, current.ClaimableBalances},
		{"LiquidityPools", snapshot.LiquidityPools, current.LiquidityPools},
		{"Contracts", snapshot.Contracts, current.Contracts},
	}

	match := true

	calcDiff := func(snapStr, curStr string) (string, bool) {
		snapInt, _ := new(big.Int).SetString(snapStr, 10)
		if snapInt == nil {
			snapInt = new(big.Int)
		}
		curInt, _ := new(big.Int).SetString(curStr, 10)
		if curInt == nil {
			curInt = new(big.Int)
		}
		diff := new(big.Int).Sub(curInt, snapInt)
		isSame := diff.Sign() == 0
		var diffStr string
		if diff.Sign() > 0 {
			diffStr = "+" + diff.String()
		} else {
			diffStr = diff.String()
		}
		return diffStr, isSame
	}

	for _, b := range buckets {
		diffStr, isSame := calcDiff(b.snap, b.cur)
		if !isSame {
			match = false
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", b.name, b.snap, b.cur, diffStr)
	}

	fmt.Fprintln(w, "------\t--------\t---------------\t----")
	totDiffStr, totSame := calcDiff(snapshot.Total, current.Total)
	if !totSame {
		match = false
	}
	fmt.Fprintf(w, "Total\t%s\t%s\t%s\n", snapshot.Total, current.Total, totDiffStr)
	w.Flush()

	fmt.Fprintln(out)
	if match {
		fmt.Fprintln(out, "Result: MATCH")
		return 0
	}
	fmt.Fprintln(out, "Result: MISMATCH")
	return 1
}
