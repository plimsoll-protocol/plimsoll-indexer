package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// contractReader reads contract state. Reconciliation never writes, so this is
// deliberately narrower than contracts: it can be satisfied without a key.
type contractReader interface {
	Read(ctx context.Context, contractID, fn string, args []xdr.ScVal) (xdr.ScVal, error)
}

// reconcileStore is the storage surface reconciliation needs.
type reconcileStore interface {
	ListAssets(ctx context.Context) ([]coverage.Asset, error)
	SaveChainSupply(ctx context.Context, sac string, sp coverage.Supply) (bool, error)
	SaveChainReport(ctx context.Context, sac string, r coverage.Report) (bool, error)
}

// Reconciler reads each listed asset's current supply snapshot and reserve
// reports back from the coverage ledger and stores anything newer than what
// ingestion has.
//
// Events are the normal source of those rows, and they carry the transaction
// that produced them. But RPC serves roughly seven days of events, so an
// ingestion gap longer than that is unrecoverable from events alone: when
// ingestion stalled for nine days the API went on serving a nine-day-old
// snapshot while the contracts held a current one. The contracts always hold
// the latest figure, so reading it back repairs the answer the API gives.
// These rows are marked 'chain' and carry no transaction hash, which is the
// honest record that they were recovered rather than observed.
//
// Reading is simulation, which needs no key and costs nothing, so this runs on
// any instance whether or not it can sign.
type Reconciler struct {
	Invoker  contractReader
	Store    reconcileStore
	LedgerID string
	Log      *slog.Logger
}

// Tiers are checked in full because a report can be posted at any of them.
var reconciledTiers = []uint32{
	coverage.TierTranscribed,
	coverage.TierIssuerSigned,
	coverage.TierAuditorSigned,
}

// Run reconciles every listed asset once. Call it on a ticker.
func (rc *Reconciler) Run(ctx context.Context) error {
	assets, err := rc.Store.ListAssets(ctx)
	if err != nil {
		return fmt.Errorf("list assets: %w", err)
	}
	var errs []error
	for _, a := range assets {
		if err := rc.reconcileOne(ctx, a.SAC, a.Code); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", a.Code, err))
		}
	}
	return errors.Join(errs...)
}

func (rc *Reconciler) reconcileOne(ctx context.Context, sac, code string) error {
	sacVal, err := soroban.AddressVal(sac)
	if err != nil {
		return err
	}

	res, err := rc.Invoker.Read(ctx, rc.LedgerID, "get_supply", []xdr.ScVal{sacVal})
	if err != nil {
		return fmt.Errorf("get_supply: %w", err)
	}
	state, err := soroban.DecodeSupplyState(res)
	if err != nil {
		return fmt.Errorf("decode get_supply: %w", err)
	}
	if state != nil {
		added, err := rc.Store.SaveChainSupply(ctx, sac, coverage.Supply{
			Amount:        state.Amount,
			Ledger:        state.Ledger,
			PostedAt:      state.Timestamp,
			BreakdownHash: state.BreakdownHash,
			Poster:        state.Poster,
		})
		if err != nil {
			return err
		}
		if added {
			rc.Log.Warn("recovered a supply snapshot from contract state that ingestion had missed",
				"code", code, "at_ledger", state.Ledger, "posted_at", state.Timestamp)
		}
	}

	var errs []error
	for _, tier := range reconciledTiers {
		res, err := rc.Invoker.Read(ctx, rc.LedgerID, "get_report", []xdr.ScVal{sacVal, soroban.U32Val(tier)})
		if err != nil {
			errs = append(errs, fmt.Errorf("get_report tier %d: %w", tier, err))
			continue
		}
		report, err := soroban.DecodeReportState(res)
		if err != nil {
			errs = append(errs, fmt.Errorf("decode get_report tier %d: %w", tier, err))
			continue
		}
		if report == nil {
			continue
		}
		added, err := rc.Store.SaveChainReport(ctx, sac, coverage.Report{
			Tier:     report.Tier,
			Amount:   report.Amount,
			AsOf:     report.AsOf,
			PostedAt: report.PostedAt,
			Reporter: report.Reporter,
			DocHash:  report.DocHash,
			DocURI:   report.DocURI,
		})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if added {
			rc.Log.Warn("recovered a reserve report from contract state that ingestion had missed",
				"code", code, "tier", coverage.TierName(report.Tier), "as_of", report.AsOf)
		}
	}
	return errors.Join(errs...)
}
