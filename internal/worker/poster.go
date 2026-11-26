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

// accountBalance is used to check the poster account's XLM reserve before posting.
type accountBalance interface {
	NativeBalance(ctx context.Context, address string) (int64, error)
}

// postLocker acquires a distributed lock so only one poster instance posts at a
// time. The returned function releases the lock.
type postLocker interface {
	TryPostLock(ctx context.Context) (func(), error)
}

// breakdownStore is the storage surface the poster needs.
type breakdownStore interface {
	ListAssets(ctx context.Context) ([]coverage.Asset, error)
	HasBreakdown(ctx context.Context, hash string) (bool, error)
	SaveBreakdown(ctx context.Context, hash, sac string, body []byte) error
	ConfirmBreakdown(ctx context.Context, hash string) error
	DeleteUnconfirmedBreakdowns(ctx context.Context, olderThan time.Duration) (int64, error)
}

// DefaultBreakdownGrace is how long a breakdown whose post was never confirmed
// is kept before it is deleted, when Poster.BreakdownGrace is zero. It is far
// longer than a transaction can stay pending, so a post that did land has
// been confirmed (by the poster reading the ledger back, by ingestion or by
// the reconciler) long before its breakdown could be deleted.
const DefaultBreakdownGrace = 24 * time.Hour

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
	Horizon supplySource
	// HorizonCheck is a second, independent Horizon the supply is compared
	// against before it is signed. Nil disables the comparison.
	HorizonCheck supplySource
	RPC          latestLedger
	Invoker      contracts
	Store        breakdownStore
	LedgerID     string
	RepostAfter  time.Duration
	Log          *slog.Logger
	Now          func() time.Time

	// BalanceChecker, when non-nil, is queried at the start of every Run tick.
	// If the poster's native XLM balance is below LowBalanceStroops a warning
	// is logged but posting continues. This is advisory-only: the poster does
	// not stop when the balance is low.
	BalanceChecker    accountBalance
	LowBalanceStroops int64

	// Lock, when non-nil, is acquired at the start of each Run tick. If the
	// lock cannot be acquired (another instance already holds it) Run logs at
	// Info and returns nil. This prevents duplicate posts when multiple
	// instances run against the same database.
	Lock postLocker

	// BreakdownGrace is how long an unconfirmed breakdown is kept before Run
	// deletes it. Zero means DefaultBreakdownGrace.
	BreakdownGrace time.Duration
}

// errSupplyDisagreement stops a post when two Horizons report different totals
// for the same ledger. The figure this indexer signs is the one thing it
// asserts on its own authority, so a second source disagreeing about it is
// reason to publish nothing rather than pick a side.
var errSupplyDisagreement = errors.New("horizons disagree about supply at the same ledger")

// crossCheck compares a breakdown against the second Horizon.
//
// Two honest Horizons at the same ledger must report the same total, so that is
// the comparison worth making: disagreement there is disagreement about a fact.
// At different ledgers they may legitimately differ because supply moved in
// between, which is not evidence of anything, so a skew is retried once and
// then reported rather than treated as a lie. Refusing to post on skew alone
// would stall a fast-moving asset indefinitely, which would be a worse failure
// than the one being guarded against.
func (p *Poster) crossCheck(ctx context.Context, b horizon.Breakdown, code, issuer string) error {
	if p.HorizonCheck == nil {
		return nil
	}
	for attempt := 1; ; attempt++ {
		other, err := p.HorizonCheck.Supply(ctx, code, issuer)
		if err != nil {
			// The check Horizon being unreachable says nothing about the figure.
			// Failing the post over it would hand any third party the power to
			// stop this indexer.
			p.Log.Warn("supply cross-check unavailable", "code", code, "err", err)
			return nil
		}
		agree := b.Total == other.Total
		if agree {
			p.Log.Debug("supply cross-checked", "code", code, "total", b.Total,
				"ledger", b.Ledger, "check_ledger", other.Ledger)
			return nil
		}
		if b.Ledger == other.Ledger {
			p.Log.Error("horizons disagree about supply", "code", code, "issuer", issuer,
				"total", b.Total, "check_total", other.Total,
				"ledger", b.Ledger, "check_ledger", other.Ledger)
			return errSupplyDisagreement
		}
		if attempt == crossCheckAttempts {
			p.Log.Warn("supply not cross-checked: horizons are at different ledgers",
				"code", code, "total", b.Total, "check_total", other.Total,
				"ledger", b.Ledger, "check_ledger", other.Ledger)
			return nil
		}
	}
}

// crossCheckAttempts is how many times the second Horizon is asked before a
// ledger skew is accepted as unverifiable. Ledgers close about every five
// seconds, so a second look is often enough for the two to line up.
const crossCheckAttempts = 2

func (p *Poster) Run(ctx context.Context) error {
	if p.Lock != nil {
		unlock, err := p.Lock.TryPostLock(ctx)
		if errors.Is(err, store.ErrLockNotAcquired) {
			p.Log.Info("poster lock held by another instance; skipping this tick")
			return nil
		}
		if err != nil {
			return fmt.Errorf("poster lock: %w", err)
		}
		defer unlock()
	}

	p.checkBalance(ctx)

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
	if err := p.pruneBreakdowns(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// pruneBreakdowns deletes the breakdowns of posts that never reached the
// chain once they are older than the grace period. They are already not
// served; this keeps them from piling up.
func (p *Poster) pruneBreakdowns(ctx context.Context) error {
	grace := p.BreakdownGrace
	if grace <= 0 {
		grace = DefaultBreakdownGrace
	}
	n, err := p.Store.DeleteUnconfirmedBreakdowns(ctx, grace)
	if err != nil {
		return fmt.Errorf("prune breakdowns: %w", err)
	}
	if n > 0 {
		p.Log.Info("deleted breakdowns whose post never reached the chain", "count", n, "older_than", grace)
	}
	return nil
}

func (p *Poster) postOne(ctx context.Context, sac, code, issuer string) error {
	b, err := p.Horizon.Supply(ctx, code, issuer)
	zeroSupply := errors.Is(err, horizon.ErrAssetNotFound)
	if zeroSupply {
		// No trustlines exist yet. The contract defines zero circulating supply
		// as u32::MAX bps (fully covered), so post a zero snapshot rather than
		// leaving the asset as 'unknown'. REPOST_AFTER still applies.
		b = zeroBreakdown(code, issuer)
	} else if err != nil {
		return err
	}

	// Get the chain head early: a zero breakdown has no ledger from Horizon,
	// and the staleness guard and the contract's ledger check both need it.
	rpcLatest, err := p.RPC.GetLatestLedger(ctx)
	if err != nil {
		return fmt.Errorf("getLatestLedger: %w", err)
	}
	if b.Ledger == 0 || b.Ledger > rpcLatest.Sequence {
		b.Ledger = rpcLatest.Sequence
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
		servable, err := p.Store.HasBreakdown(ctx, last.BreakdownHash)
		if err != nil {
			return err
		}
		if servable {
			// The ledger holds this hash, so the breakdown backs a real
			// snapshot even if the post looked like it failed or ingestion has
			// not seen its event yet.
			if err := p.Store.ConfirmBreakdown(ctx, last.BreakdownHash); err != nil {
				return err
			}
		}
		if unchanged && recent && servable {
			return nil
		}
		if b.Ledger <= last.Ledger {
			return nil // chain has not advanced past the posted snapshot yet.
		}
	}

	// Cross-check only makes sense when there is real supply to verify; a zero
	// snapshot has nothing a second Horizon can confirm or deny.
	if !zeroSupply {
		if err := p.crossCheck(ctx, b, code, issuer); err != nil {
			return err
		}
	}

	// Saved before posting, so the hash on chain always has a body here. It
	// stays unconfirmed, and is not served, until the post is known to have
	// landed.
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
		// The breakdown stays unconfirmed. If the transaction landed after all,
		// the next tick reads its hash back from the ledger and confirms it;
		// otherwise pruneBreakdowns deletes it after the grace period.
		return fmt.Errorf("post_supply: %w", err)
	}
	if err := p.Store.ConfirmBreakdown(ctx, hash); err != nil {
		return fmt.Errorf("posted supply in %s but could not confirm its breakdown: %w", res.TxHash, err)
	}
	p.Log.Info("posted supply", "code", code, "amount", b.Total, "at_ledger", b.Ledger, "tx", res.TxHash)
	return nil
}

// checkBalance logs a warning when the poster account's XLM balance is below
// the configured threshold. Errors are logged and ignored: a Horizon hiccup
// must not stop posting.
func (p *Poster) checkBalance(ctx context.Context) {
	if p.BalanceChecker == nil || p.LowBalanceStroops <= 0 {
		return
	}
	addr := p.Invoker.Address()
	bal, err := p.BalanceChecker.NativeBalance(ctx, addr)
	if err != nil {
		p.Log.Warn("could not read poster account balance", "account", addr, "err", err)
		return
	}
	if bal < p.LowBalanceStroops {
		p.Log.Warn("poster account XLM balance is low; top it up to keep posting",
			"account", addr,
			"balance_stroops", bal,
			"threshold_stroops", p.LowBalanceStroops)
	}
}

// zeroBreakdown builds an all-zero supply breakdown for an asset that Horizon
// has no record of (no trustlines created yet, or all trustlines removed).
func zeroBreakdown(code, issuer string) horizon.Breakdown {
	return horizon.Breakdown{
		Asset:                           code + ":" + issuer,
		Authorized:                      "0",
		AuthorizedToMaintainLiabilities: "0",
		Unauthorized:                    "0",
		ClaimableBalances:               "0",
		LiquidityPools:                  "0",
		Contracts:                       "0",
		Total:                           "0",
	}
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
