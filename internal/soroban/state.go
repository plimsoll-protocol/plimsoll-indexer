package soroban

import (
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// SupplyState is the coverage ledger's current snapshot for one asset, as
// get_supply returns it.
type SupplyState struct {
	Amount        *big.Int
	Ledger        uint32
	Timestamp     time.Time
	BreakdownHash string
	Poster        string
}

// DecodeSupplyState reads what get_supply returns. The contract answers
// Option<SupplySnapshot>, so an asset with nothing posted yet decodes to
// (nil, nil).
func DecodeSupplyState(v xdr.ScVal) (*SupplyState, error) {
	if v.Type == xdr.ScValTypeScvVoid {
		return nil, nil
	}
	f, err := Fields(v)
	if err != nil {
		return nil, err
	}
	amount, err := AsI128(f["amount"])
	if err != nil {
		return nil, err
	}
	ledger, err := AsU32(f["ledger"])
	if err != nil {
		return nil, err
	}
	ts, err := AsU64(f["timestamp"])
	if err != nil {
		return nil, err
	}
	hash, err := AsHex(f["breakdown_hash"])
	if err != nil {
		return nil, err
	}
	poster, err := AsAddress(f["poster"])
	if err != nil {
		return nil, err
	}
	return &SupplyState{
		Amount:        amount,
		Ledger:        ledger,
		Timestamp:     time.Unix(int64(ts), 0).UTC(),
		BreakdownHash: hash,
		Poster:        poster,
	}, nil
}
