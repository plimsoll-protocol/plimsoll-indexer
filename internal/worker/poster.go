package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/horizon"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

// contracts is the contract surface the poster needs.
type contracts interface {
	Read(ctx context.Context, contractID, fn string, args []xdr.ScVal) (xdr.ScVal, error)
	Invoke(ctx context.Context, contractID, fn string, args []xdr.ScVal) (soroban.Result, error)
	Address() string
}

// supplySource is the Horizon surface the poster needs.
type supplySource interface {
	Supply(ctx context.Context, code, issuer string) (horizon.Breakdown, error)
}

// breakdownStore is the storage surface the poster needs.
type breakdownStore interface {
	ListAssets(ctx context.Context) ([]coverage.Asset, error)
	Breakdown(ctx context.Context, hash string) ([]byte, error)
	SaveBreakdown(ctx context.Context, hash, sac string, body []byte) error
}

type latestLedger interface {
	GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error)
}

// Poster computes each listed asset's supply from Horizon and posts it to the
// coverage ledger when it changed, or when the last post is older than
// RepostAfter (so is_covered freshness checks keep passing).
//
// What counts as the last post is read from the coverage ledger itself, not
// from this indexer's tables. Those are filled by event ingestion, and when
// ingestion stalled the poster could not see its own posts land, so it
// reposted every tick for nine days and paid a fee each time.
type Poster struct {
	Horizon     supplySource
	RPC         latestLedger
	Invoker     contracts
	Store       breakdownStore
	LedgerID    string
	RepostAfter time.Duration
	Log         *slog.Logger
	Now         func() time.Time
}

func (p *Poster) Run(ctx context.Context) error {
	assets, err := p.Store.ListAssets(ctx)
	if err != nil {
		return fmt.Errorf("list assets: %w", err)
	}
	var errs []error
	for _, a := range assets {
		if err := p.postOne(ctx, a.SAC, a.Code, a.Issuer); err != nil {
			errs = append(errs, fmt.Errorf("%s:%s: %w", a.Code, a.Issuer, err))
		}
	}
	return errors.Join(errs...)
}

func (p *Poster) postOne(ctx context.Context, sac, code, issuer string) error {
	b, err := p.Horizon.Supply(ctx, code, issuer)
	if errors.Is(err, horizon.ErrAssetNotFound) {
		p.Log.Debug("asset has no holders yet", "code", code)
		return nil
	}
	if err != nil {
		return err
	}

	last, err := p.onChainSupply(ctx, sac)
	if err != nil {
		return err
	}
	if last != nil {
		unchanged := last.Amount.Cmp(b.TotalInt()) == 0
		recent := p.Now().Sub(last.Timestamp) < p.RepostAfter
		// A snapshot whose breakdown this instance cannot serve (e.g. posted by
		// another instance) is unverifiable, so replace it with one we can.
		_, err := p.Store.Breakdown(ctx, last.BreakdownHash)
		servable := err == nil
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("look up breakdown: %w", err)
		}
		if unchanged && recent && servable {
			return nil
		}
		if b.Ledger <= last.Ledger {
			return nil // Horizon has not moved past the posted snapshot yet.
		}
	}

	// The contract rejects a ledger newer than the one it is executing in.
	rpcLatest, err := p.RPC.GetLatestLedger(ctx)
	if err != nil {
		return fmt.Errorf("getLatestLedger: %w", err)
	}
	if b.Ledger > rpcLatest.Sequence {
		b.Ledger = rpcLatest.Sequence
	}

	hash := b.Hash()
	if err := p.Store.SaveBreakdown(ctx, hash, sac, b.Canonical()); err != nil {
		return fmt.Errorf("save breakdown: %w", err)
	}

	sacVal, err := soroban.AddressVal(sac)
	if err != nil {
		return err
	}
	posterVal, err := soroban.AddressVal(p.Invoker.Address())
	if err != nil {
		return err
	}
	amountVal, err := soroban.I128Val(b.TotalInt())
	if err != nil {
		return err
	}
	hashVal, err := soroban.BytesN32Val(hash)
	if err != nil {
		return err
	}
	args := []xdr.ScVal{posterVal, sacVal, amountVal, soroban.U32Val(b.Ledger), hashVal}
	res, err := p.Invoker.Invoke(ctx, p.LedgerID, "post_supply", args)
	if err != nil {
		return fmt.Errorf("post_supply: %w", err)
	}
	p.Log.Info("posted supply", "code", code, "amount", b.Total, "at_ledger", b.Ledger, "tx", res.TxHash)
	return nil
}

// onChainSupply reads the snapshot the coverage ledger currently holds for sac.
func (p *Poster) onChainSupply(ctx context.Context, sac string) (*soroban.SupplyState, error) {
	sacVal, err := soroban.AddressVal(sac)
	if err != nil {
		return nil, err
	}
	res, err := p.Invoker.Read(ctx, p.LedgerID, "get_supply", []xdr.ScVal{sacVal})
	if err != nil {
		return nil, fmt.Errorf("get_supply: %w", err)
	}
	state, err := soroban.DecodeSupplyState(res)
	if err != nil {
		return nil, fmt.Errorf("decode get_supply: %w", err)
	}
	return state, nil
}
