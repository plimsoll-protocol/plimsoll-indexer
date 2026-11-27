// Package soroban holds the small amount of XDR and RPC plumbing the indexer
// needs: encoding contract arguments, decoding event payloads, and
// submitting one contract invocation at a time.
package soroban

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// AddressVal encodes a G... or C... strkey as an ScVal address.
func AddressVal(s string) (xdr.ScVal, error) {
	addr, err := ScAddress(s)
	if err != nil {
		return xdr.ScVal{}, err
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}, nil
}

// ScAddress converts a strkey to an ScAddress.
func ScAddress(s string) (xdr.ScAddress, error) {
	switch {
	case strkey.IsValidEd25519PublicKey(s):
		aid, err := xdr.AddressToAccountId(s)
		if err != nil {
			return xdr.ScAddress{}, err
		}
		return xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeAccount, AccountId: &aid}, nil
	case strkey.IsValidContractAddress(s):
		raw, err := strkey.Decode(strkey.VersionByteContract, s)
		if err != nil {
			return xdr.ScAddress{}, err
		}
		var id xdr.ContractId
		copy(id[:], raw)
		return xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id}, nil
	default:
		return xdr.ScAddress{}, fmt.Errorf("not a G or C address: %q", s)
	}
}

// I128Val encodes a big.Int that fits in i128.
func I128Val(n *big.Int) (xdr.ScVal, error) {
	if n.Sign() < 0 || n.BitLen() > 127 {
		return xdr.ScVal{}, fmt.Errorf("value %s out of range for non-negative i128", n)
	}
	lo := new(big.Int).And(n, new(big.Int).SetUint64(^uint64(0)))
	hi := new(big.Int).Rsh(n, 64)
	parts := xdr.Int128Parts{Hi: xdr.Int64(hi.Int64()), Lo: xdr.Uint64(lo.Uint64())}
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}, nil
}

func U32Val(v uint32) xdr.ScVal {
	u := xdr.Uint32(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
}

// BytesN32Val encodes a 64-char hex string as BytesN<32>.
func BytesN32Val(hexStr string) (xdr.ScVal, error) {
	raw, err := hex.DecodeString(hexStr)
	if err != nil {
		return xdr.ScVal{}, err
	}
	if len(raw) != 32 {
		return xdr.ScVal{}, fmt.Errorf("want 32 bytes, got %d", len(raw))
	}
	b := xdr.ScBytes(raw)
	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &b}, nil
}

var errType = errors.New("unexpected scval type")

// AsSymbol returns the symbol name.
func AsSymbol(v xdr.ScVal) (string, error) {
	if v.Type != xdr.ScValTypeScvSymbol || v.Sym == nil {
		return "", fmt.Errorf("%w: want symbol, got %s", errType, v.Type)
	}
	return string(*v.Sym), nil
}

// AsAddress returns the strkey of an address value.
func AsAddress(v xdr.ScVal) (string, error) {
	if v.Type != xdr.ScValTypeScvAddress || v.Address == nil {
		return "", fmt.Errorf("%w: want address, got %s", errType, v.Type)
	}
	return v.Address.String()
}

func AsString(v xdr.ScVal) (string, error) {
	if v.Type != xdr.ScValTypeScvString || v.Str == nil {
		return "", fmt.Errorf("%w: want string, got %s", errType, v.Type)
	}
	return string(*v.Str), nil
}

func AsU32(v xdr.ScVal) (uint32, error) {
	if v.Type != xdr.ScValTypeScvU32 || v.U32 == nil {
		return 0, fmt.Errorf("%w: want u32, got %s", errType, v.Type)
	}
	return uint32(*v.U32), nil
}

func AsU64(v xdr.ScVal) (uint64, error) {
	if v.Type != xdr.ScValTypeScvU64 || v.U64 == nil {
		return 0, fmt.Errorf("%w: want u64, got %s", errType, v.Type)
	}
	return uint64(*v.U64), nil
}

// AsI128 decodes an i128 into a big.Int.
func AsI128(v xdr.ScVal) (*big.Int, error) {
	if v.Type != xdr.ScValTypeScvI128 || v.I128 == nil {
		return nil, fmt.Errorf("%w: want i128, got %s", errType, v.Type)
	}
	hi := big.NewInt(int64(v.I128.Hi))
	hi.Lsh(hi, 64)
	return hi.Add(hi, new(big.Int).SetUint64(uint64(v.I128.Lo))), nil
}

// AsHex returns a bytes value as lowercase hex.
func AsHex(v xdr.ScVal) (string, error) {
	if v.Type != xdr.ScValTypeScvBytes || v.Bytes == nil {
		return "", fmt.Errorf("%w: want bytes, got %s", errType, v.Type)
	}
	return hex.EncodeToString(*v.Bytes), nil
}

func decodeOptionalString(v xdr.ScVal, decode func(xdr.ScVal) (string, error)) (*string, error) {
	if v.Type == xdr.ScValTypeScvVoid {
		return nil, nil
	}
	value, err := decode(v)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

// Fields turns a symbol-keyed ScMap into a Go map.
func Fields(v xdr.ScVal) (map[string]xdr.ScVal, error) {
	if v.Type != xdr.ScValTypeScvMap || v.Map == nil || *v.Map == nil {
		return nil, fmt.Errorf("%w: want map, got %s", errType, v.Type)
	}
	out := make(map[string]xdr.ScVal, len(**v.Map))
	for _, entry := range **v.Map {
		key, err := AsSymbol(entry.Key)
		if err != nil {
			return nil, err
		}
		out[key] = entry.Val
	}
	return out, nil
}
