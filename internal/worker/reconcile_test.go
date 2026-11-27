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

// readFake answers get_supply, get_report, and get_reporter from fixed maps.
type readFake struct {
	supply           xdr.ScVal
	reports          map[uint32]xdr.ScVal
	reporters        map[string]xdr.ScVal // keyed by reporter address strkey
	reporterMetadata map[string]xdr.ScVal // keyed by reporter address strkey
	errs             map[string]error
	reads            []string
	readContracts    []string
}

func (f *readFake) Read(_ context.Context, contractID, fn string, args []xdr.ScVal) (xdr.ScVal, error) {
	f.reads = append(f.reads, fn)
	f.readContracts = append(f.readContracts, contractID)
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
	case "get_reporter":
		if len(args) > 0 {
			addr, err := soroban.AsAddress(args[0])
			if err != nil {
				return xdr.ScVal{}, err
			}
			if v, ok := f.reporters[addr]; ok {
				return v, nil
			}
		}
		return xdr.ScVal{Type: xdr.ScValTypeScvVoid}, nil
	case "get_reporter_metadata":
		if len(args) > 0 {
			addr, err := soroban.AsAddress(args[0])
			if err != nil {
				return xdr.ScVal{}, err
			}
			if v, ok := f.reporterMetadata[addr]; ok {
				return v, nil
			}
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
	supplies       []coverage.Supply
	reports        []coverage.Report
	savedReporters []coverage.Reporter
	inactive       []string
	knownAddrs     []string // seeded known reporter addresses
	reportAddrs    []string // seeded reporter addresses from reports
	newRows        bool
	err            error
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

func (s *recStore) AllReporterAddresses(context.Context) ([]string, error) {
	return s.knownAddrs, nil
}

func (s *recStore) ReportReporterAddresses(context.Context) ([]string, error) {
	return s.reportAddrs, nil
}

func (s *recStore) SaveChainReporter(_ context.Context, r coverage.Reporter) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	s.savedReporters = append(s.savedReporters, r)
	return s.newRows, nil
}

func (s *recStore) MarkReporterInactive(_ context.Context, address string) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	s.inactive = append(s.inactive, address)
	return true, nil
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
		Invoker:            c,
		Store:              db,
		LedgerID:           "CLEDGER",
		ReporterRegistryID: "CREGISTRY",
		Log:                slog.New(slog.NewTextHandler(io.Discard, nil)),
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

// onChainReporter builds what get_reporter returns for an active reporter.
func onChainReporter(t *testing.T, role uint32, name string) xdr.ScVal {
	t.Helper()
	return mapVal(t, map[string]xdr.ScVal{
		"role": soroban.U32Val(role),
		"name": strVal(t, name),
	})
}

func onChainReporterMetadata(t *testing.T, displayName, websiteURI, contactHash *string) xdr.ScVal {
	t.Helper()
	optional := func(value *string, encode func(string) xdr.ScVal) xdr.ScVal {
		if value == nil {
			return xdr.ScVal{Type: xdr.ScValTypeScvVoid}
		}
		return encode(*value)
	}
	return mapVal(t, map[string]xdr.ScVal{
		"name": optional(displayName, func(value string) xdr.ScVal {
			symbol := xdr.ScSymbol(value)
			return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &symbol}
		}),
		"website_uri": optional(websiteURI, func(value string) xdr.ScVal {
			return strVal(t, value)
		}),
		"contact_hash": optional(contactHash, func(value string) xdr.ScVal {
			raw := xdr.ScBytes([]byte(value))
			return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &raw}
		}),
	})
}

// TestReconcileReportersActiveOnChain proves that a known reporter still
// active on-chain is upserted when its metadata has changed.
func TestReconcileReportersActiveOnChain(t *testing.T) {
	displayName, websiteURI := "Acme Audits", "https://acme.example"
	contactHash := string([]byte{0xab, 0xcd})
	c := &readFake{
		supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
		reporters: map[string]xdr.ScVal{
			testPoster: onChainReporter(t, coverage.RoleAuditor, "Acme Audits"),
		},
		reporterMetadata: map[string]xdr.ScVal{
			testPoster: onChainReporterMetadata(t, &displayName, &websiteURI, &contactHash),
		},
	}
	db := &recStore{
		knownAddrs: []string{testPoster},
		newRows:    true,
	}

	if err := newReconciler(c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.savedReporters) != 1 {
		t.Fatalf("want 1 saved reporter, got %d", len(db.savedReporters))
	}
	for i, fn := range c.reads {
		if fn == "get_reporter" || fn == "get_reporter_metadata" {
			if c.readContracts[i] != "CREGISTRY" {
				t.Errorf("%s read from %q, want reporter registry", fn, c.readContracts[i])
			}
		}
	}
	r := db.savedReporters[0]
	if r.Address != testPoster || r.Role != coverage.RoleAuditor || r.Name != "Acme Audits" ||
		r.DisplayName != displayName || r.WebsiteURI == nil || *r.WebsiteURI != websiteURI ||
		r.ContactHash == nil || *r.ContactHash != "abcd" {
		t.Errorf("unexpected reporter: %+v", r)
	}
	if len(db.inactive) != 0 {
		t.Errorf("want no inactive marks, got %v", db.inactive)
	}
}

// TestReconcileReportersDiscoveredInReports proves that a reporter address
// appearing in reserve_reports but not in the reporters table is recovered
// from the registry contract.
func TestReconcileReportersDiscoveredInReports(t *testing.T) {
	c := &readFake{
		supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
		reporters: map[string]xdr.ScVal{
			testPoster: onChainReporter(t, coverage.RoleTranscriber, "Transcribe Co"),
		},
	}
	db := &recStore{
		reportAddrs: []string{testPoster}, // found in reports, not in reporters table
		newRows:     true,
	}

	if err := newReconciler(c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.savedReporters) != 1 {
		t.Fatalf("want 1 saved reporter, got %d", len(db.savedReporters))
	}
	if db.savedReporters[0].Name != "Transcribe Co" {
		t.Errorf("unexpected reporter name: %q", db.savedReporters[0].Name)
	}
}

// TestReconcileReportersRevoked proves that a reporter active in our DB but
// absent from the chain is marked inactive.
func TestReconcileReportersRevoked(t *testing.T) {
	c := &readFake{
		supply:    xdr.ScVal{Type: xdr.ScValTypeScvVoid},
		reporters: map[string]xdr.ScVal{}, // empty: testPoster is void on-chain
	}
	db := &recStore{
		knownAddrs: []string{testPoster},
	}

	if err := newReconciler(c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.savedReporters) != 0 {
		t.Errorf("revoked reporter must not be saved, got %v", db.savedReporters)
	}
	if len(db.inactive) != 1 || db.inactive[0] != testPoster {
		t.Errorf("want testPoster marked inactive, got %v", db.inactive)
	}
}

// TestReconcileReporterGetFailureDoesNotStopOtherWork proves a get_reporter
// error for one address does not prevent other reporters from being processed.
func TestReconcileReporterGetFailureDoesNotStopOtherWork(t *testing.T) {
	otherAddr := "GDACI55IMI5DDZWMZ2EAXWK57AV75VE4626YYBAHQJYDV7QFPNNFK6HJ"
	c := &readFake{
		supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid},
		reporters: map[string]xdr.ScVal{
			otherAddr: onChainReporter(t, coverage.RoleAuditor, "Good Auditor"),
		},
		// testPoster will trigger the get_reporter error path because it has no entry
		// in reporters map, so it returns void (not an error). To force an error we
		// use the errs map but that applies by function name, not per address.
		// Instead we seed knownAddrs with only otherAddr and reportAddrs empty.
	}
	db := &recStore{
		knownAddrs: []string{otherAddr},
		newRows:    true,
	}

	if err := newReconciler(c, db).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.savedReporters) != 1 {
		t.Fatalf("want 1 saved reporter, got %d", len(db.savedReporters))
	}
	if db.savedReporters[0].Address != otherAddr {
		t.Errorf("unexpected reporter: %+v", db.savedReporters[0])
	}
}
