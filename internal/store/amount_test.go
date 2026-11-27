package store

import (
	"math/big"
	"testing"
)

func TestParseAmount(t *testing.T) {
	huge, _ := new(big.Int).SetString("9999999999999999999999999999999999999999", 10) // NUMERIC(40, 0) maximum

	for _, tc := range []struct {
		in   string
		want *big.Int
	}{
		{"0", big.NewInt(0)},
		{"10000000000", big.NewInt(10_000_000_000)},
		{"9999999999999999999999999999999999999999", huge},
	} {
		got, err := parseAmount(tc.in)
		if err != nil {
			t.Errorf("parseAmount(%q): %v", tc.in, err)
			continue
		}
		if got.Cmp(tc.want) != 0 {
			t.Errorf("parseAmount(%q) = %s, want %s", tc.in, got, tc.want)
		}
	}
}

// A value that does not parse must fail the read. Turning it into zero would
// serve zero supply or zero reserves as if it were a real reading.
func TestParseAmountRejectsNonIntegers(t *testing.T) {
	for _, in := range []string{"", "NaN", "Infinity", "-Infinity", "12.5", "1e9", "abc", " 1"} {
		got, err := parseAmount(in)
		if err == nil {
			t.Errorf("parseAmount(%q) = %s, want an error", in, got)
			continue
		}
		if got != nil {
			t.Errorf("parseAmount(%q) returned %s alongside the error", in, got)
		}
	}
}
