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

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// readFake answers get_supply and get_report from a fixed map of tier -> value.
type readFake struct {
	supply  xdr.ScVal
	reports map[uint32]xdr.ScVal
	errs    map[string]error
	reads   []string
}

func (f *readFake) Read(_ context.Context, _, fn string, args []xdr.ScVal) (xdr.ScVal, error) {
	f.reads = append(f.reads, fn)
	if err := f.errs[fn]; err != nil {
		return xdr.ScVal{}, err
	}
	switch fn {
	case "get_supply":
		return f.supply, nil
	case "get_report":
		tier, err := soroban.AsU32(args[1])
		if err != nil {
			return xdr.ScVal{}, err
		}
		if v, ok := f.reports[tier]; ok {
			return v, nil
		}
		return xdr.ScVal{Type: xdr.ScValTypeScvVoid}, nil
	}
	return xdr.ScVal{}, errors.New("unexpected read " + fn)
}

func (f *readFake) Invoke(context.Context, string, string, []xdr.ScVal) (soroban.Result, error) {
	return soroban.Result{}, errors.New("reconciliation must not write to the chain")
}

func (f *readFake) Address() string { return testPoster }

type recStore struct {
	supplies []coverage.Supply
	reports  []coverage.Report
	newRows  bool
	err      error
}

func (s *recStore) ListAssets(context.Context) ([]coverage.Asset, error) {
	return []coverage.Asset{{SAC: testSAC, Code: "PUSD", Issuer: "GISSUER"}}, nil
}

func (s *recStore) SaveChainSupply(_ context.Context, _ string, sp coverage.Supply) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	s.supplies = append(s.supplies, sp)
	return s.newRows, nil
}

func (s *recStore) SaveChainReport(_ context.Context, _ string, r coverage.Report) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	s.reports = append(s.reports, r)
	return s.newRows, nil
}

func onChainReport(t *testing.T, tier uint32, amount int64, asOf, postedAt time.Time, uri string) xdr.ScVal {
	t.Helper()
	as := xdr.Uint64(asOf.Unix())
	posted := xdr.Uint64(postedAt.Unix())
	return mapVal(t, map[string]xdr.ScVal{
		"amount":    must(t)(soroban.I128Val(big.NewInt(amount))),
		"as_of":     {Type: xdr.ScValTypeScvU64, U64: &as},
		"posted_at": {Type: xdr.ScValTypeScvU64, U64: &posted},
		"tier":      soroban.U32Val(tier),
		"reporter":  must(t)(soroban.AddressVal(testPoster)),
		"doc_hash":  must(t)(soroban.BytesN32Val(strings.Repeat("cd", 32))),
		"doc_uri":   strVal(t, uri),
	})
}

func strVal(t *testing.T, s string) xdr.ScVal {
	t.Helper()
	v := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &v}
}

func newReconciler(c contracts, db reconcileStore) *Reconciler {
	return &Reconciler{
		Invoker:  c,
		Store:    db,
		LedgerID: "CLEDGER",
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// RPC serves about seven days of events, so a longer ingestion gap cannot be
// repaired from events at all. Contract state still holds the latest figures.
func TestReconcileRecoversCurrentStateAfterAnIngestionGap(t *testing.T) {
	at := time.Date(2026, 10, 30, 11, 0, 0, 0, time.UTC)
	c := &readFake{
		supply: onChainSnapshot(t, 9_999_000000000, 5_467_978, at, strings.Repeat("ef", 32)),
		reports: map[uint32]xdr.ScVal{
			coverage.TierIssuerSigned: onChainReport(t, coverage.TierIssuerSigned,
				10_200_000000000, at.Add(-48*time.Hour), at, "https://example.test/r.md"),
		},
	}
	db := &recStore{newRows: true}

	if err := newReconciler(c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.supplies) != 1 {
		t.Fatalf("stored %d snapshots, want 1", len(db.supplies))
	}
	got := db.supplies[0]
	if got.Ledger != 5_467_978 || got.Amount.String() != "9999000000000" || !got.PostedAt.Equal(at) {
		t.Errorf("%+v", got)
	}
	// Recovered rows cannot name the transaction that produced them.
	if got.TxHash != "" {
		t.Errorf("tx_hash = %q, want empty", got.TxHash)
	}
	if len(db.reports) != 1 {
		t.Fatalf("stored %d reports, want 1", len(db.reports))
	}
	if r := db.reports[0]; r.Tier != coverage.TierIssuerSigned || r.DocURI != "https://example.test/r.md" {
		t.Errorf("%+v", r)
	}
}

func TestReconcileStoresNothingWhenTheLedgerHasNothing(t *testing.T) {
	c := &readFake{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	db := &recStore{}
	if err := newReconciler(c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.supplies) != 0 || len(db.reports) != 0 {
		t.Errorf("stored %v and %v, want nothing", db.supplies, db.reports)
	}
}

// Every tier is checked, since a report can be posted at any of them.
func TestReconcileChecksEveryTier(t *testing.T) {
	c := &readFake{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	if err := newReconciler(c, &recStore{}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	reports := 0
	for _, fn := range c.reads {
		if fn == "get_report" {
			reports++
		}
	}
	if reports != len(reconciledTiers) {
		t.Errorf("read %d tiers, want %d", reports, len(reconciledTiers))
	}
}

// One unreadable tier must not hide the rest of the work.
func TestReconcileReportsFailuresWithoutStopping(t *testing.T) {
	at := time.Date(2026, 10, 30, 11, 0, 0, 0, time.UTC)
	c := &readFake{
		supply: onChainSnapshot(t, 1, 5_467_978, at, strings.Repeat("ef", 32)),
		errs:   map[string]error{"get_report": errors.New("boom")},
	}
	db := &recStore{newRows: true}
	err := newReconciler(c, db).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "get_report") {
		t.Fatalf("want the tier failure reported, got %v", err)
	}
	if len(db.supplies) != 1 {
		t.Errorf("the snapshot should still have been stored, got %d", len(db.supplies))
	}
}
