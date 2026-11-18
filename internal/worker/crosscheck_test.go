package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/horizon"
)

// checkHorizon is a stand-in for the second Horizon, served over HTTP so the
// real client does the talking: the cross-check is only worth anything if it
// works against something that answers like Horizon does.
//
// ledgers is consumed one per request, so a test can have the second Horizon
// catch up (or fail to) between attempts.
type checkHorizon struct {
	srv      *httptest.Server
	requests atomic.Int32
}

// newCheckHorizon serves `units` whole units of supply, reporting each ledger
// in turn and repeating the last one once they run out.
func newCheckHorizon(t *testing.T, units string, ledgers ...uint32) *checkHorizon {
	t.Helper()
	c := &checkHorizon{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			n := int(c.requests.Add(1)) - 1
			if n >= len(ledgers) {
				n = len(ledgers) - 1
			}
			fmt.Fprintf(w, `{"history_latest_ledger": %d}`, ledgers[n])
		case "/assets":
			fmt.Fprintf(w, `{"_embedded":{"records":[{
			  "asset_code":"PUSD",
			  "balances":{"authorized":"%s","authorized_to_maintain_liabilities":"0.0000000","unauthorized":"0.0000000"},
			  "claimable_balances_amount":"0.0000000",
			  "liquidity_pools_amount":"0.0000000",
			  "contracts_amount":"0.0000000"
			}]}}`, units)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *checkHorizon) client() *horizon.Client { return horizon.NewClient(c.srv.URL) }

// A primary breakdown of 999,900 PUSD, matching what newCheckHorizon serves
// when given "999900".
func crossCheckBreakdown(ledger uint32) horizon.Breakdown {
	return testBreakdown("9999000000000", ledger)
}

// newCrossPoster is newPoster with nothing already on chain, so the only thing
// that decides whether a post happens is the cross-check.
func newCrossPoster(t *testing.T, b horizon.Breakdown, check supplySource) (*Poster, *fakeContracts) {
	t.Helper()
	// Void is get_supply's answer for an asset with no snapshot yet.
	c := &fakeContracts{supply: xdr.ScVal{Type: xdr.ScValTypeScvVoid}}
	p := newPoster(&fakeHorizon{breakdown: b}, c, &fakeBreakdowns{})
	p.HorizonCheck = check
	return p, c
}

func TestCrossCheckPostsWhenBothHorizonsAgree(t *testing.T) {
	b := crossCheckBreakdown(5_469_000)
	check := newCheckHorizon(t, "999900", 5_469_000)
	p, c := newCrossPoster(t, b, check.client())

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 1 {
		t.Fatalf("want the supply posted once, got %v", c.invoked)
	}
	if got := check.requests.Load(); got != 1 {
		t.Errorf("asked the second horizon %d times, want 1 when they agree", got)
	}
}

// The attack this guards against: a Horizon reporting a smaller supply than
// the truth, which would make coverage look better than it is.
func TestCrossCheckRefusesToPostWhenHorizonsDisagreeAtTheSameLedger(t *testing.T) {
	b := crossCheckBreakdown(5_469_000)
	check := newCheckHorizon(t, "500000", 5_469_000) // half the supply, same ledger
	p, c := newCrossPoster(t, b, check.client())

	err := p.Run(context.Background())
	if !errors.Is(err, errSupplyDisagreement) {
		t.Fatalf("want errSupplyDisagreement, got %v", err)
	}
	if len(c.invoked) != 0 {
		t.Errorf("posted %v; an unverified figure must not be signed", c.invoked)
	}
}

// Two Horizons a few ledgers apart may differ honestly, because supply moved
// in between. Refusing on that alone would stall a busy asset for good, so the
// figure is posted and the log says it went unverified.
func TestCrossCheckPostsOnLedgerSkewAfterRetrying(t *testing.T) {
	b := crossCheckBreakdown(5_469_000)
	check := newCheckHorizon(t, "500000", 5_468_000, 5_468_500)
	p, c := newCrossPoster(t, b, check.client())

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := check.requests.Load(); got != crossCheckAttempts {
		t.Errorf("asked the second horizon %d times, want %d", got, crossCheckAttempts)
	}
	if len(c.invoked) != 1 {
		t.Fatalf("want the supply posted once despite the skew, got %v", c.invoked)
	}
}

// Two Horizons reporting the same total have agreed, whatever ledger each
// thinks it is on, so there is nothing a second ask could add.
func TestCrossCheckTreatsEqualTotalsAsAgreementAtAnyLedger(t *testing.T) {
	b := crossCheckBreakdown(5_469_000)
	check := newCheckHorizon(t, "999900", 5_468_000, 5_469_000) // a ledger behind
	p, c := newCrossPoster(t, b, check.client())

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := check.requests.Load(); got != 1 {
		t.Errorf("asked %d times, want 1: equal totals are agreement at any ledger", got)
	}
	if len(c.invoked) != 1 {
		t.Fatalf("want the supply posted, got %v", c.invoked)
	}
}

// A third party we cannot reach must not be able to stop this indexer posting.
func TestCrossCheckPostsWhenTheSecondHorizonIsUnreachable(t *testing.T) {
	b := crossCheckBreakdown(5_469_000)
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusBadGateway)
	}))
	defer dead.Close()
	p, c := newCrossPoster(t, b, horizon.NewClient(dead.URL))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 1 {
		t.Fatalf("want the supply posted, got %v", c.invoked)
	}
}

func TestCrossCheckIsOffWhenNoSecondHorizonIsConfigured(t *testing.T) {
	b := crossCheckBreakdown(5_469_000)
	p, c := newCrossPoster(t, b, nil)

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.invoked) != 1 {
		t.Fatalf("want the supply posted, got %v", c.invoked)
	}
}
