// Package horizon computes the circulating supply of a Stellar-issued asset
// from Horizon's /assets endpoint.
package horizon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrAssetNotFound means Horizon has no record of the asset (no trustlines yet).
var ErrAssetNotFound = errors.New("asset not found on horizon")

// Version is reported in the User-Agent sent to Horizon, so an operator seeing
// this indexer in their logs can tell which build it is. Release builds set it
// with -ldflags "-X .../internal/horizon.Version=v1.2.3".
var Version = "dev"

// Retry policy for GETs. Horizon is idempotent and public instances rate-limit,
// so a 429 or a brief 5xx is worth another try; the attempt count, the per-wait
// cap and the total budget bound how long one request may take.
const (
	maxAttempts   = 4
	baseBackoff   = 200 * time.Millisecond
	maxBackoff    = 5 * time.Second
	retryBudget   = 20 * time.Second
	maxBodyBytes  = 8 << 20 // 8 MiB
	userAgentName = "plimsoll-indexer"
)

// Breakdown is the per-bucket supply. Its canonical JSON encoding is hashed
// and the hash is posted on-chain with the total, so anyone can recompute it.
// Amounts are integer strings in the asset's smallest unit (7 decimals).
type Breakdown struct {
	Asset                           string `json:"asset"`
	Ledger                          uint32 `json:"ledger"`
	Authorized                      string `json:"authorized"`
	AuthorizedToMaintainLiabilities string `json:"authorized_to_maintain_liabilities"`
	Unauthorized                    string `json:"unauthorized"`
	ClaimableBalances               string `json:"claimable_balances"`
	LiquidityPools                  string `json:"liquidity_pools"`
	Contracts                       string `json:"contracts"`
	Total                           string `json:"total"`
}

// Canonical returns the exact bytes that are hashed.
func (b Breakdown) Canonical() []byte {
	out, _ := json.Marshal(b) // struct of strings and a uint32 cannot fail
	return out
}

// Hash is the hex sha256 of Canonical().
func (b Breakdown) Hash() string {
	sum := sha256.Sum256(b.Canonical())
	return hex.EncodeToString(sum[:])
}

// TotalInt returns Total as a big.Int.
func (b Breakdown) TotalInt() *big.Int {
	n, _ := new(big.Int).SetString(b.Total, 10)
	return n
}

type Client struct {
	BaseURL string
	HTTP    *http.Client

	// sleep waits between retries; nil means a real timer that also watches
	// ctx. It is a field so tests do not spend the backoff in wall time.
	sleep func(context.Context, time.Duration) error
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 20 * time.Second},
	}
}

type AssetRecord struct {
	Balances struct {
		Authorized                      string `json:"authorized"`
		AuthorizedToMaintainLiabilities string `json:"authorized_to_maintain_liabilities"`
		Unauthorized                    string `json:"unauthorized"`
	} `json:"balances"`
	ClaimableBalancesAmount string `json:"claimable_balances_amount"`
	LiquidityPoolsAmount    string `json:"liquidity_pools_amount"`
	ContractsAmount         string `json:"contracts_amount"`
}

type root struct {
	HistoryLatestLedger uint32 `json:"history_latest_ledger"`
}

type account struct {
	HomeDomain string `json:"home_domain"`
	Balances   []struct {
		AssetType string `json:"asset_type"`
		Balance   string `json:"balance"`
	} `json:"balances"`
}

// LatestLedger returns the newest ledger Horizon has ingested.
func (c *Client) LatestLedger(ctx context.Context) (uint32, error) {
	var r root
	if err := c.get(ctx, "/", &r); err != nil {
		return 0, fmt.Errorf("horizon root: %w", err)
	}
	return r.HistoryLatestLedger, nil
}

// HomeDomain returns the issuer account's home_domain ("" if unset).
func (c *Client) HomeDomain(ctx context.Context, issuer string) (string, error) {
	var a account
	if err := c.get(ctx, "/accounts/"+url.PathEscape(issuer), &a); err != nil {
		return "", fmt.Errorf("horizon account %s: %w", issuer, err)
	}
	return a.HomeDomain, nil
}

// NativeBalance returns the XLM balance of address in stroops (1 XLM = 10 000 000 stroops).
// Returns 0 and ErrAssetNotFound when the account does not exist.
func (c *Client) NativeBalance(ctx context.Context, address string) (int64, error) {
	var a account
	if err := c.get(ctx, "/accounts/"+url.PathEscape(address), &a); err != nil {
		return 0, fmt.Errorf("horizon account %s: %w", address, err)
	}
	for _, b := range a.Balances {
		if b.AssetType == "native" {
			n, err := ParseAmount(b.Balance)
			if err != nil {
				return 0, fmt.Errorf("parse native balance: %w", err)
			}
			return n.Int64(), nil
		}
	}
	return 0, nil
}

// Supply fetches the asset record and sums every bucket that is a claim on
// the issuer: trustline balances in all authorization states, claimable
// balances, liquidity-pool reserves and balances held by contracts.
func (c *Client) Supply(ctx context.Context, code, issuer string) (Breakdown, error) {
	ledger, err := c.LatestLedger(ctx)
	if err != nil {
		return Breakdown{}, err
	}
	q := url.Values{"asset_code": {code}, "asset_issuer": {issuer}}
	var page struct {
		Embedded struct {
			Records []AssetRecord `json:"records"`
		} `json:"_embedded"`
	}
	if err := c.get(ctx, "/assets?"+q.Encode(), &page); err != nil {
		return Breakdown{}, fmt.Errorf("horizon assets %s:%s: %w", code, issuer, err)
	}
	if len(page.Embedded.Records) == 0 {
		return Breakdown{}, ErrAssetNotFound
	}
	return BreakdownFromRecord(code+":"+issuer, ledger, page.Embedded.Records[0])
}

// BreakdownFromRecord converts Horizon's decimal strings to integer units.
func BreakdownFromRecord(asset string, ledger uint32, r AssetRecord) (Breakdown, error) {
	type bucket struct {
		name string
		in   string
		out  *string
	}
	b := Breakdown{Asset: asset, Ledger: ledger}
	fields := []bucket{
		{"authorized", r.Balances.Authorized, &b.Authorized},
		{"authorized_to_maintain_liabilities", r.Balances.AuthorizedToMaintainLiabilities, &b.AuthorizedToMaintainLiabilities},
		{"unauthorized", r.Balances.Unauthorized, &b.Unauthorized},
		{"claimable_balances_amount", r.ClaimableBalancesAmount, &b.ClaimableBalances},
		{"liquidity_pools_amount", r.LiquidityPoolsAmount, &b.LiquidityPools},
		{"contracts_amount", r.ContractsAmount, &b.Contracts},
	}
	total := new(big.Int)
	for _, f := range fields {
		n, err := ParseAmount(f.in)
		if err != nil {
			return Breakdown{}, fmt.Errorf("%s: %w", f.name, err)
		}
		*f.out = n.String()
		total.Add(total, n)
	}
	b.Total = total.String()
	return b, nil
}

// ParseAmount converts a Horizon decimal string ("12.3400000") to an integer
// count of the smallest unit (7 decimals). An empty string is zero.
func ParseAmount(s string) (*big.Int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return new(big.Int), nil
	}
	if strings.HasPrefix(s, "-") {
		return nil, fmt.Errorf("negative amount %q", s)
	}
	whole, frac, _ := strings.Cut(s, ".")
	if len(frac) > 7 {
		return nil, fmt.Errorf("amount %q has more than 7 decimals", s)
	}
	frac += strings.Repeat("0", 7-len(frac))
	if whole == "" {
		whole = "0"
	}
	n, ok := new(big.Int).SetString(whole+frac, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount %q", s)
	}
	return n, nil
}

// get performs an idempotent GET, retrying rate limits and server errors within
// a bounded number of attempts and a bounded total time, and honouring
// Retry-After when Horizon sends one. Client errors other than 429 are not
// retried: repeating a request Horizon has rejected as wrong will not fix it.
func (c *Client) get(ctx context.Context, path string, out any) error {
	deadline := time.Now().Add(retryBudget)
	for attempt := 1; ; attempt++ {
		retryAfter, retryable, err := c.getOnce(ctx, path, out)
		if err == nil {
			return nil
		}
		if !retryable {
			return err
		}
		if attempt == maxAttempts {
			return fmt.Errorf("%s: giving up after %d attempts: %w", path, attempt, err)
		}
		wait := backoffDelay(attempt, retryAfter)
		if time.Now().Add(wait).After(deadline) {
			return fmt.Errorf("%s: retry budget of %s exhausted: %w", path, retryBudget, err)
		}
		if err := c.wait(ctx, wait); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
}

// getOnce makes one attempt. It reports whether the failure is worth retrying
// and, if Horizon said so, how long to wait first.
func (c *Client) getOnce(ctx context.Context, path string, out any) (retryAfter time.Duration, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgentName+"/"+Version)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK:
		return 0, false, decodeLimited(resp.Body, out)
	case resp.StatusCode == http.StatusNotFound:
		return 0, false, ErrAssetNotFound
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return parseRetryAfter(resp.Header, time.Now()), true,
			fmt.Errorf("unexpected status %d", resp.StatusCode)
	default:
		return 0, false, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}

// decodeLimited decodes a response body of at most maxBodyBytes. Horizon pages
// are small; a body larger than that means the URL is wrong or the peer is not
// Horizon, and reading it whole would let a bad endpoint decide this process's
// memory use.
func decodeLimited(body io.Reader, out any) error {
	b, err := io.ReadAll(io.LimitReader(body, maxBodyBytes+1))
	if err != nil {
		return err
	}
	if len(b) > maxBodyBytes {
		return fmt.Errorf("response body exceeds %d bytes", maxBodyBytes)
	}
	return json.Unmarshal(b, out)
}

// parseRetryAfter reads Horizon's Retry-After, in seconds or as an HTTP date.
// A missing, malformed or past value yields zero, which falls back to backoff.
func parseRetryAfter(hdr http.Header, now time.Time) time.Duration {
	v := strings.TrimSpace(hdr.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// backoffDelay returns how long to wait before the next attempt. A Retry-After
// is honoured as sent: the caller gives up when it does not fit the retry
// budget, rather than retrying early and being rate-limited again. Otherwise it
// is capped exponential backoff with equal jitter, so instances that were
// rate-limited together do not all come back at the same instant.
func backoffDelay(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	d := baseBackoff
	for i := 1; i < attempt && d < maxBackoff; i++ {
		d *= 2
	}
	d = min(d, maxBackoff)
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// wait sleeps for d, returning early with ctx's error if the context ends.
func (c *Client) wait(ctx context.Context, d time.Duration) error {
	if c.sleep != nil {
		return c.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
