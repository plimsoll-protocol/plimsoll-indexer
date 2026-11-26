package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/horizon"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

var _ postLocker = (*fakeLock)(nil)

const (
	testSAC    = "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"
	testPoster = "GDACI55IMI5DDZWMZ2EAXWK57AV75VE4626YYBAHQJYDV7QFPNNFK6HJ"
)

var postedAt = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

type fakeContracts struct {
	supply    xdr.ScVal // what get_supply returns
	readErr   error
	invokeErr error // what post_supply returns
	invoked   []string
	postArgs  [][]xdr.ScVal
}

func (f *fakeContracts) Read(_ context.Context, _, fn string, _ []xdr.ScVal) (xdr.ScVal, error) {
	if f.readErr != nil {
		return xdr.ScVal{}, f.readErr
	}
	if fn != "get_supply" {
		return xdr.ScVal{}, errors.New("unexpected read " + fn)
	}
	return f.supply, nil
}

func (f *fakeContracts) Invoke(_ context.Context, _, fn string, args []xdr.ScVal) (soroban.Result, error) {
	f.invoked = append(f.invoked, fn)
	f.postArgs = append(f.postArgs, args)
	if f.invokeErr != nil {
		return soroban.Result{}, f.invokeErr
	}
	return soroban.Result{TxHash: "deadbeef"}, nil
}

func (f *fakeContracts) Address() string { return testPoster }

type fakeHorizon struct {
	breakdown horizon.Breakdown
	err       error
}

func (f *fakeHorizon) Supply(context.Context, string, string) (horizon.Breakdown, error) {
	return f.breakdown, f.err
}

type fakeBreakdowns struct {
	have      map[string]bool
	saved     []string
	confirmed []string
	pruned    []time.Duration // olderThan of each DeleteUnconfirmedBreakdowns call
	pruneErr  error
}

func (f *fakeBreakdowns) ListAssets(context.Context) ([]coverage.Asset, error) {
	return []coverage.Asset{{SAC: testSAC, Code: "PUSD", Issuer: "GISSUER"}}, nil
}

func (f *fakeBreakdowns) HasBreakdown(_ context.Context, hash string) (bool, error) {
	return f.have[hash], nil
}

func (f *fakeBreakdowns) SaveBreakdown(_ context.Context, hash, _ string, _ []byte) error {
	f.saved = append(f.saved, hash)
	return nil
}

func (f *fakeBreakdowns) ConfirmBreakdown(_ context.Context, hash string) error {
	f.confirmed = append(f.confirmed, hash)
	return nil
}

func (f *fakeBreakdowns) DeleteUnconfirmedBreakdowns(_ context.Context, olderThan time.Duration) (int64, error) {
	f.pruned = append(f.pruned, olderThan)
	return 0, f.pruneErr
}

type fakeLedger struct{ seq uint32 }

func (f *fakeLedger) GetLatestLedger(context.Context) (protocol.GetLatestLedgerResponse, error) {
	return protocol.GetLatestLedgerResponse{Sequence: f.seq}, nil
}

// onChainSnapshot builds what get_supply returns for an already posted snapshot.
func onChainSnapshot(t *testing.T, amount int64, ledger uint32, at time.Time, hash string) xdr.ScVal {
	t.Helper()
	ts := xdr.Uint64(at.Unix())
	return mapVal(t, map[string]xdr.ScVal{
		"amount":         must(t)(soroban.I128Val(big.NewInt(amount))),
		"ledger":         soroban.U32Val(ledger),
		"timestamp":      {Type: xdr.ScValTypeScvU64, U64: &ts},
		"breakdown_hash": must(t)(soroban.BytesN32Val(hash)),
		"poster":         must(t)(soroban.AddressVal(testPoster)),
	})
}

func testBreakdown(total string, ledger uint32) horizon.Breakdown {
	return horizon.Breakdown{Asset: "PUSD", Ledger: ledger, Total: total, Authorized: total}
}

func newPoster(h *fakeHorizon, c *fakeContracts, db *fakeBreakdowns) *Poster {
	return &Poster{
		Horizon:     h,
		RPC:         &fakeLedger{seq: 5_470_000},
		Invoker:     c,
		Store:       db,
		LedgerID:    "CLEDGER",
		RepostAfter: 6 * time.Hour,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:         func() time.Time { return postedAt },
	}
}

// The poster used to read the last post back from this indexer's own tables,
// which event ingestion fills. While ingestion was stalled it never saw its
// posts land and reposted on every tick, paying a fee each time. The decision
// has to come from the coverage ledger.
func TestPosterSkipsWhenTheChainSnapshotIsFresh(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	db := &fakeBreakdowns{have: map[string]bool{b.Hash(): true}}
	c := &fakeContracts{supply: onChainSnapshot(t, 9_999_000000000, 5_468_000, postedAt.Add(-time.Hour), b.Hash())}

	if err := newPoster(&fakeHorizon{breakdown: b}, c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 0 {
		t.Errorf("posted %v, want nothing posted", c.invoked)
	}
}

func TestPosterRepostsWhenTheChainSnapshotIsStale(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	db := &fakeBreakdowns{have: map[string]bool{b.Hash(): true}}
	c := &fakeContracts{supply: onChainSnapshot(t, 9_999_000000000, 5_468_000, postedAt.Add(-7*time.Hour), b.Hash())}

	if err := newPoster(&fakeHorizon{breakdown: b}, c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 1 || c.invoked[0] != "post_supply" {
		t.Fatalf("invoked %v, want one post_supply", c.invoked)
	}
	if len(db.saved) != 1 || db.saved[0] != b.Hash() {
		t.Errorf("saved breakdowns %v, want the posted hash", db.saved)
	}
}

func TestPosterPostsWhenTheAmountChanged(t *testing.T) {
	b := testBreakdown("12000000000000", 5_469_000)
	db := &fakeBreakdowns{have: map[string]bool{b.Hash(): true}}
	c := &fakeContracts{supply: onChainSnapshot(t, 9_999_000000000, 5_468_000, postedAt.Add(-time.Minute), b.Hash())}

	if err := newPoster(&fakeHorizon{breakdown: b}, c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 1 {
		t.Fatalf("invoked %v, want one post", c.invoked)
	}
}

// A snapshot whose breakdown this instance cannot serve is unverifiable by
// anyone reading the API, so it is replaced even while it is fresh.
func TestPosterRepostsWhenTheBreakdownIsNotServable(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	db := &fakeBreakdowns{}
	c := &fakeContracts{supply: onChainSnapshot(t, 9_999_000000000, 5_468_000, postedAt.Add(-time.Minute), strings.Repeat("ab", 32))}

	if err := newPoster(&fakeHorizon{breakdown: b}, c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 1 {
		t.Fatalf("invoked %v, want one post", c.invoked)
	}
}

func TestPosterPostsFirstSnapshotWhenNothingIsOnChain(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}

	if err := newPoster(&fakeHorizon{breakdown: b}, c, &fakeBreakdowns{}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 1 {
		t.Fatalf("invoked %v, want the first post", c.invoked)
	}
}

func TestPosterClampsLedgerToTheChainHead(t *testing.T) {
	// Horizon can be ahead of the ledger the contract executes in, and the
	// contract rejects a snapshot from the future.
	b := testBreakdown("9999000000000", 5_480_000)
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	p := newPoster(&fakeHorizon{breakdown: b}, c, &fakeBreakdowns{})

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	ledger, err := soroban.AsU32(c.postArgs[0][3])
	if err != nil {
		t.Fatal(err)
	}
	if ledger != 5_470_000 {
		t.Errorf("posted at ledger %d, want it clamped to 5470000", ledger)
	}
}

func TestPosterReportsAChainReadFailure(t *testing.T) {
	c := &fakeContracts{readErr: errors.New("simulate get_supply: boom")}
	err := newPoster(&fakeHorizon{breakdown: testBreakdown("1", 5_469_000)}, c, &fakeBreakdowns{}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "get_supply") {
		t.Fatalf("want the read failure reported, got %v", err)
	}
	if len(c.invoked) != 0 {
		t.Error("nothing should be posted when the chain could not be read")
	}
}

// --- balance-warning tests (#44) -------------------------------------------

type fakeBalanceChecker struct {
	bal int64
	err error
}

func (f *fakeBalanceChecker) NativeBalance(_ context.Context, _ string) (int64, error) {
	return f.bal, f.err
}

func TestPosterLogsWarningWhenBalanceBelowThreshold(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	b := testBreakdown("9999000000000", 5_469_000)
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	p := newPoster(&fakeHorizon{breakdown: b}, c, &fakeBreakdowns{})
	p.Log = log
	p.BalanceChecker = &fakeBalanceChecker{bal: 500_000} // 0.05 XLM
	p.LowBalanceStroops = 10_000_000                     // 1 XLM threshold
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "balance is low") {
		t.Errorf("expected a low-balance warning, got log: %s", buf.String())
	}
}

func TestPosterNoWarningWhenBalanceAboveThreshold(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	b := testBreakdown("9999000000000", 5_469_000)
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	p := newPoster(&fakeHorizon{breakdown: b}, c, &fakeBreakdowns{})
	p.Log = log
	p.BalanceChecker = &fakeBalanceChecker{bal: 50_000_000} // 5 XLM
	p.LowBalanceStroops = 10_000_000                        // 1 XLM threshold
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "balance is low") {
		t.Errorf("unexpected low-balance warning, got log: %s", buf.String())
	}
}

func TestPosterContinuesWhenBalanceCheckFails(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	p := newPoster(&fakeHorizon{breakdown: b}, c, &fakeBreakdowns{})
	p.BalanceChecker = &fakeBalanceChecker{err: errors.New("horizon timeout")}
	p.LowBalanceStroops = 10_000_000
	// Should not return an error — balance check failure is advisory only.
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run returned an error after balance check failure: %v", err)
	}
	if len(c.invoked) != 1 {
		t.Errorf("expected posting to continue despite balance error, invoked: %v", c.invoked)
	}
}

// --- advisory lock tests (#45) ---------------------------------------------

type fakeLock struct {
	acquired bool // true = lock is free and can be acquired
	unlocked int
}

func (f *fakeLock) TryPostLock(_ context.Context) (func(), error) {
	if !f.acquired {
		return nil, store.ErrLockNotAcquired
	}
	return func() { f.unlocked++ }, nil
}

func TestPosterSkipsWhenLockIsHeld(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	p := newPoster(&fakeHorizon{breakdown: b}, c, &fakeBreakdowns{})
	p.Lock = &fakeLock{acquired: false}
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(c.invoked) != 0 {
		t.Errorf("posted %v when lock was held; expected nothing posted", c.invoked)
	}
}

func TestPosterPostsWhenLockIsAcquired(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	lock := &fakeLock{acquired: true}
	p := newPoster(&fakeHorizon{breakdown: b}, c, &fakeBreakdowns{})
	p.Lock = lock
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if len(c.invoked) != 1 {
		t.Errorf("invoked %v, want one post when lock is acquired", c.invoked)
	}
	if lock.unlocked != 1 {
		t.Errorf("unlock called %d times, want 1", lock.unlocked)
	}
}

// TestPosterPostsZeroSnapshotForAssetWithNoHolders proves that an asset with
// no trustlines gets a zero snapshot posted (u32::MAX bps = fully covered)
// rather than being left as 'unknown'. Fixes #46.
func TestPosterPostsZeroSnapshotForAssetWithNoHolders(t *testing.T) {
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	h := &fakeHorizon{err: horizon.ErrAssetNotFound}
	if err := newPoster(h, c, &fakeBreakdowns{}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 1 || c.invoked[0] != "post_supply" {
		t.Fatalf("invoked %v, want one post_supply for the zero snapshot", c.invoked)
	}
	// Confirm amount arg is zero.
	amount, err := soroban.AsI128(c.postArgs[0][2])
	if err != nil {
		t.Fatal(err)
	}
	if amount.Sign() != 0 {
		t.Errorf("posted amount = %s, want 0", amount)
	}
}

// TestPosterRepostsZeroSnapshotForFormerlyHeldAsset proves that an asset whose
// supply has since dropped to zero (ErrAssetNotFound) is reposted when the
// existing on-chain snapshot is stale. Fixes #46.
func TestPosterRepostsZeroSnapshotForFormerlyHeldAsset(t *testing.T) {
	zero := zeroBreakdown("PUSD", "GISSUER")
	db := &fakeBreakdowns{have: map[string]bool{zero.Hash(): true}}
	c := &fakeContracts{supply: onChainSnapshot(t, 0, 5_469_000, postedAt.Add(-7*time.Hour), zero.Hash())}
	h := &fakeHorizon{err: horizon.ErrAssetNotFound}
	if err := newPoster(h, c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 1 || c.invoked[0] != "post_supply" {
		t.Fatalf("invoked %v, want one post_supply when the zero snapshot is stale", c.invoked)
	}
}

// TestPosterSkipsZeroSnapshotWhenFresh proves that REPOST_AFTER applies to
// zero snapshots the same way it does to non-zero ones. Fixes #46.
func TestPosterSkipsZeroSnapshotWhenFresh(t *testing.T) {
	zero := zeroBreakdown("PUSD", "GISSUER")
	db := &fakeBreakdowns{have: map[string]bool{zero.Hash(): true}}
	c := &fakeContracts{supply: onChainSnapshot(t, 0, 5_469_000, postedAt.Add(-time.Minute), zero.Hash())}
	h := &fakeHorizon{err: horizon.ErrAssetNotFound}
	if err := newPoster(h, c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 0 {
		t.Errorf("invoked %v, want nothing (zero snapshot is fresh)", c.invoked)
	}
}

// A successful post confirms its breakdown, so the API starts serving it.
func TestPosterConfirmsTheBreakdownOfASuccessfulPost(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	db := &fakeBreakdowns{}
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	if err := newPoster(&fakeHorizon{breakdown: b}, c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.saved) != 1 || db.saved[0] != b.Hash() {
		t.Fatalf("saved %v, want [%s]", db.saved, b.Hash())
	}
	if len(db.confirmed) != 1 || db.confirmed[0] != b.Hash() {
		t.Fatalf("confirmed %v, want [%s]", db.confirmed, b.Hash())
	}
}

// A post that fails leaves its breakdown saved but unconfirmed: never served,
// and deleted once the grace period passes.
func TestPosterLeavesTheBreakdownOfAFailedPostUnconfirmed(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	db := &fakeBreakdowns{}
	c := &fakeContracts{
		supply:    xdr.ScVal{Type: xdr.ScValTypeScvVoid},
		invokeErr: errors.New("send post_supply: status ERROR"),
	}
	err := newPoster(&fakeHorizon{breakdown: b}, c, db).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "post_supply") {
		t.Fatalf("err = %v, want the post_supply failure", err)
	}
	if len(db.saved) != 1 || db.saved[0] != b.Hash() {
		t.Fatalf("saved %v, want [%s]", db.saved, b.Hash())
	}
	if len(db.confirmed) != 0 {
		t.Fatalf("confirmed %v after a failed post", db.confirmed)
	}
	if len(db.pruned) != 1 {
		t.Fatalf("pruned %d times, want once even when a post fails", len(db.pruned))
	}
}

// A post that looked like it failed may still land. The next tick reads the
// hash back from the ledger and confirms the breakdown it already has.
func TestPosterConfirmsTheBreakdownTheLedgerHolds(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	db := &fakeBreakdowns{have: map[string]bool{b.Hash(): true}}
	c := &fakeContracts{supply: onChainSnapshot(t, 9_999_000000000, 5_468_000, postedAt.Add(-time.Hour), b.Hash())}
	if err := newPoster(&fakeHorizon{breakdown: b}, c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 0 {
		t.Fatalf("invoked %v; the fresh snapshot needs no repost", c.invoked)
	}
	if len(db.confirmed) != 1 || db.confirmed[0] != b.Hash() {
		t.Fatalf("confirmed %v, want [%s]", db.confirmed, b.Hash())
	}
}

func TestPosterPrunesUnconfirmedBreakdowns(t *testing.T) {
	b := testBreakdown("9999000000000", 5_469_000)
	snapshot := func() *fakeContracts {
		return &fakeContracts{supply: onChainSnapshot(t, 9_999_000000000, 5_468_000, postedAt.Add(-time.Hour), b.Hash())}
	}

	db := &fakeBreakdowns{have: map[string]bool{b.Hash(): true}}
	if err := newPoster(&fakeHorizon{breakdown: b}, snapshot(), db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.pruned) != 1 || db.pruned[0] != DefaultBreakdownGrace {
		t.Fatalf("pruned %v, want [%s]", db.pruned, DefaultBreakdownGrace)
	}

	db = &fakeBreakdowns{have: map[string]bool{b.Hash(): true}}
	p := newPoster(&fakeHorizon{breakdown: b}, snapshot(), db)
	p.BreakdownGrace = 2 * time.Hour
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.pruned) != 1 || db.pruned[0] != 2*time.Hour {
		t.Fatalf("pruned %v, want [2h]", db.pruned)
	}

	db = &fakeBreakdowns{have: map[string]bool{b.Hash(): true}, pruneErr: errors.New("connection reset")}
	err := newPoster(&fakeHorizon{breakdown: b}, snapshot(), db).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "prune breakdowns: connection reset") {
		t.Fatalf("err = %v, want the prune failure reported", err)
	}
}
