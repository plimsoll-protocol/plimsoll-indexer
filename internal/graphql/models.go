package graphql

import "time"

// Asset is the GraphQL model for a listed asset. It mirrors the generated
// shape plus MinTier, which carries the parent assets(minTier: ...) argument
// into the coverage field when the client does not override it there.
type Asset struct {
	Sac           string
	Code          string
	Issuer        string
	ListedLedger  int
	ListedAt      time.Time
	Toml          *string
	TomlCheckedAt *time.Time
	TomlError     *string
	Supply        *Supply
	Reports       []*Report
	Coverage      *Coverage
	MinTier       int
}
