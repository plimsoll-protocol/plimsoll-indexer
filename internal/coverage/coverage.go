// Package coverage holds the indexer's data model and mirrors the
// coverage-ledger contract's selection and basis-point rules, so the API
// gives the same answer as the chain.
package coverage

import (
	"math"
	"math/big"
	"time"
)

// Tiers, matching plimsoll_types::Tier.
const (
	TierTranscribed   uint32 = 1
	TierIssuerSigned  uint32 = 2
	TierAuditorSigned uint32 = 3
)

// Roles, matching plimsoll_types::Role.
const (
	RoleAuditor      uint32 = 1
	RoleTranscriber  uint32 = 2
	RoleSupplyPoster uint32 = 3
)

func TierName(t uint32) string {
	switch t {
	case TierTranscribed:
		return "transcribed"
	case TierIssuerSigned:
		return "issuer_signed"
	case TierAuditorSigned:
		return "auditor_signed"
	default:
		return "unknown"
	}
}

func RoleName(r uint32) string {
	switch r {
	case RoleAuditor:
		return "auditor"
	case RoleTranscriber:
		return "transcriber"
	case RoleSupplyPoster:
		return "supply_poster"
	default:
		return "unknown"
	}
}

type Asset struct {
	SAC           string     `json:"sac"`
	Code          string     `json:"code"`
	Issuer        string     `json:"issuer"`
	ListedLedger  uint32     `json:"listed_ledger"`
	ListedAt      time.Time  `json:"listed_at"`
	Toml          []byte     `json:"-"`
	TomlCheckedAt *time.Time `json:"toml_checked_at,omitempty"`
	TomlError     string     `json:"toml_error,omitempty"`
}

type Supply struct {
	Amount        *big.Int  `json:"-"`
	Ledger        uint32    `json:"ledger"`
	PostedAt      time.Time `json:"posted_at"`
	BreakdownHash string    `json:"breakdown_hash"`
	Poster        string    `json:"poster"`
	TxHash        string    `json:"tx_hash"`
	// Source is "event" for a figure decoded from a contract event, which
	// carries the transaction above, or "chain" for one read back from contract
	// state after an ingestion gap, where tx_hash is empty.
	Source string `json:"source"`
}

type Report struct {
	Tier     uint32    `json:"-"`
	Amount   *big.Int  `json:"-"`
	AsOf     time.Time `json:"as_of"`
	PostedAt time.Time `json:"posted_at"`
	Reporter string    `json:"reporter"`
	DocHash  string    `json:"doc_hash"`
	DocURI   string    `json:"doc_uri"`
	TxHash   string    `json:"tx_hash"`
	Source   string    `json:"source"`
}

type Reporter struct {
	Address   string    `json:"address"`
	Role      uint32    `json:"-"`
	Name      string    `json:"name"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Result mirrors the contract's Coverage struct.
type Result struct {
	Bps            uint32
	Tier           uint32
	Supply         *big.Int
	Reserves       *big.Int
	SupplyLedger   uint32
	SupplyPostedAt time.Time
	ReportAsOf     time.Time
	ReportPostedAt time.Time
	ReportReporter string
	ReportDocURI   string
	NothingIsOwed  bool
}

// BPS is reserves * 10_000 / supply, rounded down, saturating at MaxUint32.
// Zero supply returns MaxUint32, as on-chain.
func BPS(reserves, supply *big.Int) uint32 {
	if supply.Sign() <= 0 {
		return math.MaxUint32
	}
	if reserves.Sign() < 0 {
		return 0
	}
	q := new(big.Int).Mul(reserves, big.NewInt(10_000))
	q.Quo(q, supply)
	if !q.IsUint64() || q.Uint64() > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(q.Uint64())
}

// Best picks, among tiers >= minTier, the report with the latest AsOf; ties
// go to the stronger tier. Same rule as coverage-ledger.
func Best(latestPerTier map[uint32]Report, minTier uint32) (Report, bool) {
	var best Report
	found := false
	for _, tier := range []uint32{TierAuditorSigned, TierIssuerSigned, TierTranscribed} {
		if tier < minTier {
			continue
		}
		r, ok := latestPerTier[tier]
		if !ok {
			continue
		}
		if !found || r.AsOf.After(best.AsOf) {
			best, found = r, true
		}
	}
	return best, found
}

// Compute returns nil when there is no supply snapshot or no qualifying report.
func Compute(supply *Supply, latestPerTier map[uint32]Report, minTier uint32) *Result {
	if supply == nil {
		return nil
	}
	r, ok := Best(latestPerTier, minTier)
	if !ok {
		return nil
	}
	return &Result{
		Bps:            BPS(r.Amount, supply.Amount),
		Tier:           r.Tier,
		Supply:         supply.Amount,
		Reserves:       r.Amount,
		SupplyLedger:   supply.Ledger,
		SupplyPostedAt: supply.PostedAt,
		ReportAsOf:     r.AsOf,
		ReportPostedAt: r.PostedAt,
		ReportReporter: r.Reporter,
		ReportDocURI:   r.DocURI,
		NothingIsOwed:  supply.Amount.Sign() == 0,
	}
}

// Covered mirrors is_covered: both inputs no older than maxAge and bps >= minBps.
func (r *Result) Covered(now time.Time, minBps uint32, maxAge time.Duration) bool {
	if r == nil {
		return false
	}
	fresh := func(t time.Time) bool { return now.Sub(t) <= maxAge }
	return fresh(r.SupplyPostedAt) && fresh(r.ReportAsOf) && r.Bps >= minBps
}
