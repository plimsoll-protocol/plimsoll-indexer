package soroban

import (
	"math/big"
	"strings"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	gAddr = "GBIE3ANCRVCBWETUZXWYKRMP27LQVJHX757XXAQUT3LYHVTNFPPPXEY4"
	cAddr = "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"
)

func TestAddressRoundTrip(t *testing.T) {
	for _, s := range []string{gAddr, cAddr} {
		v, err := AddressVal(s)
		if err != nil {
			t.Fatal(err)
		}
		back, err := AsAddress(v)
		if err != nil || back != s {
			t.Fatalf("%s -> %s (%v)", s, back, err)
		}
	}
	if _, err := AddressVal("nope"); err == nil {
		t.Fatal("expected error")
	}
}

func TestI128RoundTrip(t *testing.T) {
	big1, _ := new(big.Int).SetString("3505136135138075", 10)
	huge := new(big.Int).Lsh(big.NewInt(1), 100)
	for _, n := range []*big.Int{big.NewInt(0), big.NewInt(1), big1, huge} {
		v, err := I128Val(n)
		if err != nil {
			t.Fatal(err)
		}
		back, err := AsI128(v)
		if err != nil || back.Cmp(n) != 0 {
			t.Fatalf("%s -> %s (%v)", n, back, err)
		}
	}
	if _, err := I128Val(big.NewInt(-1)); err == nil {
		t.Fatal("negative accepted")
	}
	if _, err := I128Val(new(big.Int).Lsh(big.NewInt(1), 127)); err == nil {
		t.Fatal("overflow accepted")
	}
}

func TestBytesN32(t *testing.T) {
	h := strings.Repeat("ab", 32)
	v, err := BytesN32Val(h)
	if err != nil {
		t.Fatal(err)
	}
	back, _ := AsHex(v)
	if back != h {
		t.Fatal(back)
	}
	if _, err := BytesN32Val("abcd"); err == nil {
		t.Fatal("short hash accepted")
	}
}

func sym(s string) xdr.ScVal {
	v := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &v}
}

func str(s string) xdr.ScVal {
	v := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &v}
}

func u64(n uint64) xdr.ScVal {
	v := xdr.Uint64(n)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &v}
}

func mapVal(kv ...any) xdr.ScVal {
	m := xdr.ScMap{}
	for i := 0; i < len(kv); i += 2 {
		m = append(m, xdr.ScMapEntry{Key: sym(kv[i].(string)), Val: kv[i+1].(xdr.ScVal)})
	}
	mp := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mp}
}

func b64(t *testing.T, v xdr.ScVal) string {
	t.Helper()
	s, err := xdr.MarshalBase64(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func check(t *testing.T) func(xdr.ScVal, error) xdr.ScVal {
	return func(v xdr.ScVal, err error) xdr.ScVal {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func TestDecodeReservePosted(t *testing.T) {
	sac := check(t)(AddressVal(cAddr))
	reporter := check(t)(AddressVal(gAddr))
	amount := check(t)(I128Val(big.NewInt(1_020_0000000)))
	hash := check(t)(BytesN32Val(strings.Repeat("01", 32)))
	ev := protocol.EventInfo{
		ID:              "0001-0",
		Ledger:          5034500,
		LedgerClosedAt:  "2026-10-05T10:30:00Z",
		ContractID:      "CLEDGER",
		TransactionHash: "abc",
		TopicXDR:        []string{b64(t, sym("reserve_posted")), b64(t, sac), b64(t, U32Val(3))},
		ValueXDR: b64(t, mapVal(
			"amount", amount,
			"as_of", u64(1_700_000_000),
			"doc_hash", hash,
			"doc_uri", str("https://x/y.pdf"),
			"reporter", reporter,
		)),
	}
	got, err := Decode(ev)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := got.(ReservePosted)
	if !ok {
		t.Fatalf("got %T", got)
	}
	if r.SAC != cAddr || r.Tier != 3 || r.Reporter != gAddr || r.DocURI != "https://x/y.pdf" {
		t.Errorf("%+v", r)
	}
	if r.Amount.Int64() != 1_020_0000000 {
		t.Errorf("amount %s", r.Amount)
	}
	if !r.AsOf.Equal(time.Unix(1_700_000_000, 0)) {
		t.Errorf("as_of %s", r.AsOf)
	}
	if r.Ledger != 5034500 || r.TxHash != "abc" {
		t.Errorf("meta %+v", r.Meta)
	}
}

func TestDecodeAssetListed(t *testing.T) {
	ev := protocol.EventInfo{
		ID:       "2",
		TopicXDR: []string{b64(t, sym("asset_listed")), b64(t, check(t)(AddressVal(cAddr)))},
		ValueXDR: b64(t, mapVal("code", str("PUSD"), "issuer", check(t)(AddressVal(gAddr)))),
	}
	got, err := Decode(ev)
	if err != nil {
		t.Fatal(err)
	}
	a := got.(AssetListed)
	if a.SAC != cAddr || a.Code != "PUSD" || a.Issuer != gAddr {
		t.Errorf("%+v", a)
	}
}

func TestDecodeMissingFieldIsError(t *testing.T) {
	ev := protocol.EventInfo{
		ID:       "3",
		TopicXDR: []string{b64(t, sym("asset_listed")), b64(t, check(t)(AddressVal(cAddr)))},
		ValueXDR: b64(t, mapVal("code", str("PUSD"))),
	}
	if _, err := Decode(ev); err == nil {
		t.Fatal("expected error for missing issuer")
	}
}

func TestDecodeIgnoresUnknownEvents(t *testing.T) {
	ev := protocol.EventInfo{ID: "4", TopicXDR: []string{b64(t, sym("admin_transferred"))}}
	got, err := Decode(ev)
	if err != nil || got != nil {
		t.Fatalf("got %v, %v", got, err)
	}
}

// FuzzAsI128 checks the i128 encoding round-trips for every in-range value
// and that anything out of range is refused rather than truncated.
func FuzzAsI128(f *testing.F) {
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(0), ^uint64(0))
	f.Add(uint64(1)<<62, uint64(12345))
	f.Add(^uint64(0)>>1, ^uint64(0))
	f.Add(^uint64(0), uint64(0))
	f.Fuzz(func(t *testing.T, hi, lo uint64) {
		n := new(big.Int).Lsh(new(big.Int).SetUint64(hi), 64)
		n.Or(n, new(big.Int).SetUint64(lo))
		v, err := I128Val(n)
		if n.BitLen() > 127 {
			if err == nil {
				t.Fatalf("I128Val accepted out-of-range %s", n)
			}
			return
		}
		if err != nil {
			t.Fatalf("I128Val(%s): %v", n, err)
		}
		back, err := AsI128(v)
		if err != nil || back.Cmp(n) != 0 {
			t.Fatalf("%s -> %v (%v)", n, back, err)
		}
	})
}

// FuzzDecode feeds arbitrary base64 topics and values to Decode. It must never
// panic, and a nil error with a supply or reserve event must carry an amount.
func FuzzDecode(f *testing.F) {
	addr, _ := AddressVal(cAddr)
	issuer, _ := AddressVal(gAddr)
	amt, _ := I128Val(big.NewInt(3505136135138075))
	hash, _ := BytesN32Val(strings.Repeat("ab", 32))
	u32 := U32Val(7)
	enc := func(v xdr.ScVal) string {
		s, err := xdr.MarshalBase64(v)
		if err != nil {
			f.Fatal(err)
		}
		return s
	}
	f.Add(enc(sym("asset_listed")), enc(addr), "", enc(mapVal("code", str("PUSD"), "issuer", issuer)))
	f.Add(enc(sym("supply_posted")), enc(addr), "", enc(mapVal("amount", amt, "ledger", u32, "breakdown_hash", hash, "poster", issuer)))
	f.Add(enc(sym("reserve_posted")), enc(addr), enc(u32), enc(mapVal("amount", amt, "as_of", u64(1700000000), "reporter", issuer, "doc_hash", hash, "doc_uri", str("https://x"))))
	f.Add(enc(sym("reporter_set")), enc(issuer), "", enc(mapVal("role", u32, "name", str("n"))))
	f.Add(enc(sym("reporter_revoked")), enc(issuer), "", "")
	f.Add("", "", "", "")
	f.Add("not base64!", "AAAA", "AAAA", "AAAA")
	f.Fuzz(func(t *testing.T, t0, t1, t2, value string) {
		var topics []string
		for _, s := range []string{t0, t1, t2} {
			if s != "" {
				topics = append(topics, s)
			}
		}
		got, err := Decode(protocol.EventInfo{ID: "fuzz", TopicXDR: topics, ValueXDR: value, LedgerClosedAt: "2025-01-01T00:00:00Z"})
		if err != nil {
			return
		}
		switch e := got.(type) {
		case SupplyPosted:
			if e.Amount == nil {
				t.Fatal("SupplyPosted without amount")
			}
		case ReservePosted:
			if e.Amount == nil {
				t.Fatal("ReservePosted without amount")
			}
		}
	})
}
