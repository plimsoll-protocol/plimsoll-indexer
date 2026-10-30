package soroban

import (
	"fmt"
	"math/big"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Event names emitted by the Plimsoll contracts (snake_case struct names).
const (
	EventAssetListed     = "asset_listed"
	EventSupplyPosted    = "supply_posted"
	EventReservePosted   = "reserve_posted"
	EventReporterSet     = "reporter_set"
	EventReporterRevoked = "reporter_revoked"
)

// Meta is common to every event.
type Meta struct {
	ID       string
	Ledger   uint32
	ClosedAt time.Time
	TxHash   string
	Contract string
}

// EventMeta returns the metadata every Plimsoll event carries, so callers can
// track ingest progress without switching on the concrete type.
func (m Meta) EventMeta() Meta { return m }

// WithMeta is satisfied by every event Decode returns.
type WithMeta interface{ EventMeta() Meta }

type AssetListed struct {
	Meta
	SAC, Code, Issuer string
}

type SupplyPosted struct {
	Meta
	SAC           string
	Amount        *big.Int
	AtLedger      uint32
	BreakdownHash string
	Poster        string
}

type ReservePosted struct {
	Meta
	SAC      string
	Tier     uint32
	Amount   *big.Int
	AsOf     time.Time
	Reporter string
	DocHash  string
	DocURI   string
}

type ReporterSet struct {
	Meta
	Reporter string
	Role     uint32
	Name     string
}

type ReporterRevoked struct {
	Meta
	Reporter string
}

// Decode turns an RPC event into one of the typed events above. Events this
// indexer does not track return (nil, nil).
func Decode(e protocol.EventInfo) (any, error) {
	if len(e.TopicXDR) == 0 {
		return nil, nil
	}
	topics := make([]xdr.ScVal, len(e.TopicXDR))
	for i, t := range e.TopicXDR {
		if err := xdr.SafeUnmarshalBase64(t, &topics[i]); err != nil {
			return nil, fmt.Errorf("event %s topic %d: %w", e.ID, i, err)
		}
	}
	name, err := AsSymbol(topics[0])
	if err != nil {
		return nil, nil
	}
	var value xdr.ScVal
	if e.ValueXDR != "" {
		if err := xdr.SafeUnmarshalBase64(e.ValueXDR, &value); err != nil {
			return nil, fmt.Errorf("event %s value: %w", e.ID, err)
		}
	}
	closedAt, _ := time.Parse(time.RFC3339, e.LedgerClosedAt)
	meta := Meta{
		ID:       e.ID,
		Ledger:   uint32(e.Ledger),
		ClosedAt: closedAt.UTC(),
		TxHash:   e.TransactionHash,
		Contract: e.ContractID,
	}

	d := decoder{topics: topics}
	switch name {
	case EventAssetListed:
		f := d.fields(value)
		out := AssetListed{Meta: meta, SAC: d.topicAddr(1)}
		out.Code = d.str(f, "code")
		out.Issuer = d.addr(f, "issuer")
		return out, d.wrap(e.ID, name)
	case EventSupplyPosted:
		f := d.fields(value)
		out := SupplyPosted{Meta: meta, SAC: d.topicAddr(1)}
		out.Amount = d.i128(f, "amount")
		out.AtLedger = d.u32(f, "ledger")
		out.BreakdownHash = d.hex(f, "breakdown_hash")
		out.Poster = d.addr(f, "poster")
		return out, d.wrap(e.ID, name)
	case EventReservePosted:
		f := d.fields(value)
		out := ReservePosted{Meta: meta, SAC: d.topicAddr(1), Tier: d.topicU32(2)}
		out.Amount = d.i128(f, "amount")
		out.AsOf = time.Unix(int64(d.u64(f, "as_of")), 0).UTC()
		out.Reporter = d.addr(f, "reporter")
		out.DocHash = d.hex(f, "doc_hash")
		out.DocURI = d.str(f, "doc_uri")
		return out, d.wrap(e.ID, name)
	case EventReporterSet:
		f := d.fields(value)
		out := ReporterSet{Meta: meta, Reporter: d.topicAddr(1)}
		out.Role = d.u32(f, "role")
		out.Name = d.str(f, "name")
		return out, d.wrap(e.ID, name)
	case EventReporterRevoked:
		out := ReporterRevoked{Meta: meta, Reporter: d.topicAddr(1)}
		return out, d.wrap(e.ID, name)
	default:
		return nil, nil
	}
}

// decoder records the first error so the switch above stays readable.
type decoder struct {
	topics []xdr.ScVal
	err    error
}

func (d *decoder) wrap(id, name string) error {
	if d.err != nil {
		return fmt.Errorf("event %s (%s): %w", id, name, d.err)
	}
	return nil
}

func (d *decoder) set(err error) {
	if d.err == nil && err != nil {
		d.err = err
	}
}

func (d *decoder) topic(i int) xdr.ScVal {
	if i >= len(d.topics) {
		d.set(fmt.Errorf("missing topic %d", i))
		return xdr.ScVal{}
	}
	return d.topics[i]
}

func (d *decoder) topicAddr(i int) string {
	s, err := AsAddress(d.topic(i))
	d.set(err)
	return s
}

func (d *decoder) topicU32(i int) uint32 {
	v, err := AsU32(d.topic(i))
	d.set(err)
	return v
}

func (d *decoder) fields(v xdr.ScVal) map[string]xdr.ScVal {
	f, err := Fields(v)
	d.set(err)
	return f
}

func (d *decoder) field(f map[string]xdr.ScVal, k string) xdr.ScVal {
	v, ok := f[k]
	if !ok {
		d.set(fmt.Errorf("missing field %q", k))
	}
	return v
}

func (d *decoder) str(f map[string]xdr.ScVal, k string) string {
	s, err := AsString(d.field(f, k))
	d.set(err)
	return s
}

func (d *decoder) addr(f map[string]xdr.ScVal, k string) string {
	s, err := AsAddress(d.field(f, k))
	d.set(err)
	return s
}

func (d *decoder) u32(f map[string]xdr.ScVal, k string) uint32 {
	v, err := AsU32(d.field(f, k))
	d.set(err)
	return v
}

func (d *decoder) u64(f map[string]xdr.ScVal, k string) uint64 {
	v, err := AsU64(d.field(f, k))
	d.set(err)
	return v
}

func (d *decoder) i128(f map[string]xdr.ScVal, k string) *big.Int {
	v, err := AsI128(d.field(f, k))
	d.set(err)
	if v == nil {
		return new(big.Int)
	}
	return v
}

func (d *decoder) hex(f map[string]xdr.ScVal, k string) string {
	s, err := AsHex(d.field(f, k))
	d.set(err)
	return s
}
