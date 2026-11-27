package soroban

import (
	"fmt"
	"math/big"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// ReporterState is the registry contract's current entry for one reporter, as
// get_reporter returns it.
type ReporterState struct {
	Role uint32
	Name string
}

type ReporterMetadataState struct {
	DisplayName *string
	WebsiteURI  *string
	ContactHash *string
}

// DecodeReporterState reads what get_reporter returns. A reporter that has
// never been registered, or was revoked, decodes to (nil, nil).
func DecodeReporterState(v xdr.ScVal) (*ReporterState, error) {
	if v.Type == xdr.ScValTypeScvVoid {
		return nil, nil
	}
	f, err := Fields(v)
	if err != nil {
		return nil, err
	}
	role, err := AsU32(f["role"])
	if err != nil {
		return nil, err
	}
	name, err := AsString(f["name"])
	if err != nil {
		return nil, err
	}
	return &ReporterState{Role: role, Name: name}, nil
}

// DecodeReporterMetadata reads what get_reporter_metadata returns. A reporter
// with no published metadata decodes to (nil, nil).
func DecodeReporterMetadata(v xdr.ScVal) (*ReporterMetadataState, error) {
	if v.Type == xdr.ScValTypeScvVoid {
		return nil, nil
	}
	f, err := Fields(v)
	if err != nil {
		return nil, err
	}
	name, err := optionalMetadataField(f, "name", AsSymbol)
	if err != nil {
		return nil, err
	}
	websiteURI, err := optionalMetadataField(f, "website_uri", AsString)
	if err != nil {
		return nil, err
	}
	contactHash, err := optionalMetadataField(f, "contact_hash", AsHex)
	if err != nil {
		return nil, err
	}
	return &ReporterMetadataState{
		DisplayName: name,
		WebsiteURI:  websiteURI,
		ContactHash: contactHash,
	}, nil
}

func optionalMetadataField(fields map[string]xdr.ScVal, name string, decode func(xdr.ScVal) (string, error)) (*string, error) {
	value, ok := fields[name]
	if !ok {
		return nil, fmt.Errorf("missing field %q", name)
	}
	return decodeOptionalString(value, decode)
}

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

// ReportState is one of the coverage ledger's current reserve reports, as
// get_report returns it.
type ReportState struct {
	Amount   *big.Int
	AsOf     time.Time
	PostedAt time.Time
	Tier     uint32
	Reporter string
	DocHash  string
	DocURI   string
}

// DecodeReportState reads what get_report returns. A tier with nothing posted
// decodes to (nil, nil).
func DecodeReportState(v xdr.ScVal) (*ReportState, error) {
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
	asOf, err := AsU64(f["as_of"])
	if err != nil {
		return nil, err
	}
	postedAt, err := AsU64(f["posted_at"])
	if err != nil {
		return nil, err
	}
	tier, err := AsU32(f["tier"])
	if err != nil {
		return nil, err
	}
	reporter, err := AsAddress(f["reporter"])
	if err != nil {
		return nil, err
	}
	docHash, err := AsHex(f["doc_hash"])
	if err != nil {
		return nil, err
	}
	docURI, err := AsString(f["doc_uri"])
	if err != nil {
		return nil, err
	}
	return &ReportState{
		Amount:   amount,
		AsOf:     time.Unix(int64(asOf), 0).UTC(),
		PostedAt: time.Unix(int64(postedAt), 0).UTC(),
		Tier:     tier,
		Reporter: reporter,
		DocHash:  docHash,
		DocURI:   docURI,
	}, nil
}
