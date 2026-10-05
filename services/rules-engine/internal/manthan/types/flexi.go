package types

// Flexi caps — "Provable-Void Release" (design brief 2026-09-30, §2).
//
// When one mcap bucket has provably NO opportunity for a strategy today, the
// slots it would otherwise hold under its 50% base cap may be lent to a
// bucket that has more eligible names than base room. Everything here is
// PURE (no I/O, no clock reads, no logging): the caller supplies a universe
// snapshot, the portfolio, the env-derived config and the IST wall clock,
// and gets back either a FlexiPlan or nil + a guard reason.
//
// Invariants this file guarantees, independent of the caller:
//   - Base caps are never lowered: a ceiling is recorded only when > Base.
//   - The total cap is untouched: Σ grants ≤ Free = N − ΣHeld − MIN_IDLE.
//   - Non-void buckets keep their OWN base room (BaseClaim is reserved
//     before any Extra is handed out) → order-independent end state GIVEN
//     THE DONOR SET AT FIRST ARRIVAL (see below).
//   - Ceiling = max(Base, Held) + Extra with Extra shrinking exactly as Held
//     grows → a receiver's ceiling never drops while it is being filled.
//   - Any uncertainty about the universe → nil plan (fail closed to base).
//
// Order-independence precondition. The end state is arrival-order
// independent whenever some allowed donor is ALREADY void when the first
// receiver signal arrives (the S4450 trace: LARGE has 0 eligible names at
// 09:00). It is NOT when a donor bucket becomes void only intraday because
// its last eligible names were BOUGHT: with Opp LARGE = 1 and DONORS=LARGE,
// SMALL signals consumed before the LARGE one find LARGE non-void → no donor
// → all blocked at base; if the LARGE name is consumed first it is bought,
// LARGE flips to void and later SMALL signals borrow. Kafka partition
// interleaving decides which. Every individual grant is still valid (the
// donor was provably void at grant time, all invariants hold) — the effect
// is UNDER-borrowing, never a wrong grant — but dry-run evidence will show
// day-to-day variance on such days that is not a bug. Pinned by
// TestBuildFlexiPlan_OrderDependenceWhenDonorFlipsIntraday. Whether the donor
// gate should also admit a bucket whose remaining Opp ≤ its base room is an
// owner decision (design §7.2), deliberately not taken here.
//
// Nothing is persisted: every ceiling is a function of Positions, the
// signals_db rows for the signal's run_date, the strategy's CreatedAt and
// env. A restart mid-day recomputes the identical plan.

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// FlexiMode is the MANTHAN_FLEXI_CAPS_MODE value.
type FlexiMode string

const (
	FlexiOff    FlexiMode = "off"     // no DB fetch, nil input, byte-identical to pre-flexi
	FlexiDryRun FlexiMode = "dry_run" // plan computed + FLEXI_EVAL rows; real decisions use base caps
	FlexiOn     FlexiMode = "on"      // plan applied to the cap check
)

// Canonical mcap bucket strings (data-ingestion writes exactly these).
const (
	BucketLarge = "LARGE"
	BucketMid   = "MID"
	BucketSmall = "SMALL"
)

// CanonicalBuckets in the default priority order.
var CanonicalBuckets = []string{BucketLarge, BucketMid, BucketSmall}

// Env keys. Every one of these must be forwarded by the PM2 env block
// (deployments/separate-namespace/ecosystem.config.js — env blocks are
// whitelists) or the feature silently never runs.
const (
	EnvFlexiMode              = "MANTHAN_FLEXI_CAPS_MODE"
	EnvFlexiPriority          = "MANTHAN_FLEXI_PRIORITY"
	EnvFlexiDonors            = "MANTHAN_FLEXI_DONORS"
	EnvFlexiMaxReceiverPct    = "MANTHAN_FLEXI_MAX_RECEIVER_PCT"
	EnvFlexiMinIdleSlots      = "MANTHAN_FLEXI_MIN_IDLE_SLOTS"
	EnvFlexiMinUniverseRows   = "MANTHAN_FLEXI_MIN_UNIVERSE_ROWS"
	EnvFlexiCutoffIST         = "MANTHAN_FLEXI_CUTOFF_IST"
	EnvFlexiStrategyAllowlist = "MANTHAN_FLEXI_STRATEGY_ALLOWLIST"
	EnvFlexiDBTimeoutMs       = "MANTHAN_FLEXI_DB_TIMEOUT_MS"
)

// FlexiEnvKeys is the complete list of env keys the feature reads.
var FlexiEnvKeys = []string{
	EnvFlexiMode, EnvFlexiPriority, EnvFlexiDonors, EnvFlexiMaxReceiverPct,
	EnvFlexiMinIdleSlots, EnvFlexiMinUniverseRows, EnvFlexiCutoffIST,
	EnvFlexiStrategyAllowlist, EnvFlexiDBTimeoutMs,
}

// FlexiConfig is the parsed env surface. The zero value is NOT valid config;
// use ParseFlexiConfig / LoadFlexiConfigFromEnv (defaults applied there).
type FlexiConfig struct {
	Mode            FlexiMode
	Priority        []string // receiver walk order — a permutation of CanonicalBuckets
	Donors          []string // buckets allowed to donate when void
	MaxReceiverPct  int      // any bucket's effective ceiling ≤ floor(N·pct/100); 50–100
	MinIdleSlots    int      // slots never lent
	MinUniverseRows int      // manthan_stocks rows for D required before any bucket may be void
	CutoffIST       string   // "HH:MM" — no new grants at/after
	Allowlist       []string // strategy_ids (lower-cased); empty = all
	DBTimeout       time.Duration

	// Warnings collected while parsing (caller logs them). ForcedOff is set
	// when a non-off mode was requested but an invalid value made it unsafe.
	Warnings  []string
	ForcedOff bool

	cutoffMinute int
	allow        map[string]struct{}
}

// Enabled reports whether any flexi work (universe fetch, plan, evals) runs.
func (c FlexiConfig) Enabled() bool {
	return c.Mode == FlexiDryRun || c.Mode == FlexiOn
}

// AppliesTo reports whether the strategy is in the allowlist (empty = all).
func (c FlexiConfig) AppliesTo(strategyID string) bool {
	if len(c.allow) == 0 {
		return true
	}
	_, ok := c.allow[strings.ToLower(strings.TrimSpace(strategyID))]
	return ok
}

// ReceiverCap is floor(N · MaxReceiverPct / 100).
func (c FlexiConfig) ReceiverCap(maxPositions int) int {
	return maxPositions * c.MaxReceiverPct / 100
}

// CutoffReached reports whether nowIST is at/after CutoffIST. A zero time
// (unknown clock) is treated as "not reached" only because minute 0 < any
// cutoff — callers always pass a real IST clock.
func (c FlexiConfig) CutoffReached(nowIST time.Time) bool {
	return nowIST.Hour()*60+nowIST.Minute() >= c.cutoffMinute
}

// Summary is the config echo written into every FLEXI_EVAL payload.
func (c FlexiConfig) Summary() map[string]any {
	return map[string]any{
		"mode":              string(c.Mode),
		"priority":          c.Priority,
		"donors":            c.Donors,
		"max_receiver_pct":  c.MaxReceiverPct,
		"min_idle_slots":    c.MinIdleSlots,
		"min_universe_rows": c.MinUniverseRows,
		"cutoff_ist":        c.CutoffIST,
		"allowlist":         c.Allowlist,
		"db_timeout_ms":     c.DBTimeout.Milliseconds(),
	}
}

// LoadFlexiConfigFromEnv parses the process environment.
func LoadFlexiConfigFromEnv() FlexiConfig {
	return ParseFlexiConfig(os.Getenv)
}

// ParseFlexiConfig parses the flexi env surface via lookup (os.Getenv in
// production, a map in tests). Unset/empty keys take their defaults. An
// unrecognised mode, or ANY invalid value while a non-off mode is requested,
// forces Mode=off and records a warning — never a guessed value.
func ParseFlexiConfig(lookup func(string) string) FlexiConfig {
	get := func(k string) string { return strings.TrimSpace(lookup(k)) }
	c := FlexiConfig{
		Mode:            FlexiOff,
		Priority:        append([]string(nil), CanonicalBuckets...),
		Donors:          append([]string(nil), CanonicalBuckets...),
		MaxReceiverPct:  80,
		MinIdleSlots:    1,
		MinUniverseRows: 5,
		CutoffIST:       "15:20",
		DBTimeout:       300 * time.Millisecond,
		cutoffMinute:    15*60 + 20,
	}
	warn := func(format string, a ...any) { c.Warnings = append(c.Warnings, fmt.Sprintf(format, a...)) }
	invalid := false

	switch m := strings.ToLower(get(EnvFlexiMode)); m {
	case "", "off":
		c.Mode = FlexiOff
	case "dry_run":
		c.Mode = FlexiDryRun
	case "on":
		c.Mode = FlexiOn
	default:
		warn("%s=%q unrecognised (off|dry_run|on) — mode forced off", EnvFlexiMode, m)
		c.Mode = FlexiOff
		invalid = true
	}

	if v := get(EnvFlexiPriority); v != "" {
		list, ok := parseBucketList(v)
		if !ok || len(list) != len(CanonicalBuckets) {
			warn("%s=%q must be a permutation of LARGE,MID,SMALL", EnvFlexiPriority, v)
			invalid = true
		} else {
			c.Priority = list
		}
	}
	if v := get(EnvFlexiDonors); v != "" {
		list, ok := parseBucketList(v)
		if !ok || len(list) == 0 {
			warn("%s=%q must be a non-empty subset of LARGE,MID,SMALL", EnvFlexiDonors, v)
			invalid = true
		} else {
			c.Donors = list
		}
	}
	if v := get(EnvFlexiMaxReceiverPct); v != "" {
		n, err := strconv.Atoi(v)
		switch {
		case err != nil:
			warn("%s=%q not an integer", EnvFlexiMaxReceiverPct, v)
			invalid = true
		case n < 50:
			warn("%s=%d below 50 — clamped to 50", EnvFlexiMaxReceiverPct, n)
			c.MaxReceiverPct = 50
		case n > 100:
			warn("%s=%d above 100 — clamped to 100", EnvFlexiMaxReceiverPct, n)
			c.MaxReceiverPct = 100
		default:
			c.MaxReceiverPct = n
		}
	}
	if v := get(EnvFlexiMinIdleSlots); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			warn("%s=%q must be an integer ≥ 0", EnvFlexiMinIdleSlots, v)
			invalid = true
		} else {
			c.MinIdleSlots = n
		}
	}
	if v := get(EnvFlexiMinUniverseRows); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			warn("%s=%q must be an integer ≥ 1", EnvFlexiMinUniverseRows, v)
			invalid = true
		} else {
			c.MinUniverseRows = n
		}
	}
	if v := get(EnvFlexiCutoffIST); v != "" {
		if t, err := time.Parse("15:04", v); err != nil {
			warn("%s=%q must be HH:MM", EnvFlexiCutoffIST, v)
			invalid = true
		} else {
			c.CutoffIST = t.Format("15:04")
			c.cutoffMinute = t.Hour()*60 + t.Minute()
		}
	}
	if v := get(EnvFlexiStrategyAllowlist); v != "" {
		c.allow = map[string]struct{}{}
		for _, s := range strings.Split(v, ",") {
			s = strings.ToLower(strings.TrimSpace(s))
			if s == "" {
				continue
			}
			if _, dup := c.allow[s]; !dup {
				c.allow[s] = struct{}{}
				c.Allowlist = append(c.Allowlist, s)
			}
		}
		if len(c.allow) == 0 {
			warn("%s=%q contains no strategy ids", EnvFlexiStrategyAllowlist, v)
			invalid = true
		}
	}
	if v := get(EnvFlexiDBTimeoutMs); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			warn("%s=%q must be a positive integer (ms)", EnvFlexiDBTimeoutMs, v)
			invalid = true
		} else {
			c.DBTimeout = time.Duration(n) * time.Millisecond
		}
	}

	if invalid && c.Mode != FlexiOff {
		warn("invalid flexi configuration — %s forced off (was %s)", EnvFlexiMode, c.Mode)
		c.Mode = FlexiOff
		c.ForcedOff = true
	}
	return c
}

// parseBucketList parses "LARGE, mid,SMALL" into canonical unique buckets.
// ok=false on an unknown or duplicated bucket.
func parseBucketList(v string) ([]string, bool) {
	seen := map[string]bool{}
	var out []string
	for _, part := range strings.Split(v, ",") {
		b := strings.ToUpper(strings.TrimSpace(part))
		if b == "" {
			continue
		}
		if !IsCanonicalBucket(b) || seen[b] {
			return nil, false
		}
		seen[b] = true
		out = append(out, b)
	}
	return out, true
}

// IsCanonicalBucket reports whether b is exactly LARGE, MID or SMALL.
func IsCanonicalBucket(b string) bool {
	return b == BucketLarge || b == BucketMid || b == BucketSmall
}

// NormalizeBucket is UPPER(TRIM(b)) — the key form used for universe rows.
func NormalizeBucket(b string) string {
	return strings.ToUpper(strings.TrimSpace(b))
}

// ────────────────────────────────────────────────────────────────────
// Universe snapshot (signals_db, one REPEATABLE READ tx per Kafka message)
// ────────────────────────────────────────────────────────────────────

// FlexiUniverseRow is one manthan_signals row for the run_date.
type FlexiUniverseRow struct {
	Symbol      string
	Industry    string
	Bucket      string    // UPPER(TRIM(COALESCE(mcap_bucket,'')))
	FirstSeenAt time.Time // zero when NULL (creation gate fails open, like the consumer)
}

// FlexiUniverse is the per-run_date snapshot the plan is judged against.
// A non-nil Err means the fetch failed or was inconsistent → universe
// unknown → every plan fails closed to base caps.
type FlexiUniverse struct {
	RunDate              string
	StocksRows           int            // count(*) manthan_stocks WHERE run_date=D
	StocksByStatus       map[string]int // ELIGIBLE / FILTER_REJECTED / DATA_DROPPED …
	Eligible             []FlexiUniverseRow
	InfraUnknown         map[string]bool // buckets with an infra-downgraded FILTER_REJECTED row
	SnapshotMaxCreatedAt time.Time
	QueriedAt            time.Time
	QueryMs              int64
	Err                  error
}

// FlexiUniverseSummary is what the plan records about its evidence.
type FlexiUniverseSummary struct {
	StocksRows           int            `json:"stocks_rows"`
	EligibleRows         int            `json:"eligible_rows"`
	StocksByStatus       map[string]int `json:"stocks_by_status"`
	SnapshotMaxCreatedAt string         `json:"snapshot_max_created_at"`
	QueriedAt            string         `json:"queried_at"`
	QueryMs              int64          `json:"query_ms"`
}

// ShadowEntry is a dry-run WOULD_ALLOCATE decision from earlier today —
// what on-mode would have bought. Overlaid onto the caps so dry-run predicts
// on-mode instead of re-granting the same slot to every signal.
type ShadowEntry struct {
	Symbol   string
	Industry string
	Bucket   string
}

// FlexiInput is everything the allocator needs to build a plan for one
// strategy on one Allocate call. nil ⇒ feature off ⇒ byte-identical path.
type FlexiInput struct {
	Cfg               FlexiConfig
	Universe          *FlexiUniverse
	StrategyCreatedAt time.Time
	NowIST            time.Time
	Shadow            []ShadowEntry // dry-run only
}

// ────────────────────────────────────────────────────────────────────
// Plan
// ────────────────────────────────────────────────────────────────────

// Guard reasons — the string a nil plan comes with. Fail-closed reasons
// (universe uncertainty) warrant a rate-limited Warn; structural reasons
// ("nothing to lend") are the normal case and only counted.
const (
	FlexiGuardModeOff         = "mode_off"
	FlexiGuardNoSignals       = "no_signals"
	FlexiGuardCutoff          = "cutoff_reached"
	FlexiGuardRunDateEmpty    = "run_date_empty"
	FlexiGuardRunDateMismatch = "run_date_mismatch"
	FlexiGuardUniverseNil     = "universe_unavailable"
	FlexiGuardUniverseError   = "universe_error"
	FlexiGuardUniverseRows    = "universe_rows_below_min"
	FlexiGuardBucketNonCanon  = "signal_bucket_noncanonical"
	FlexiGuardNotInSnapshot   = "signal_not_in_snapshot"
	FlexiGuardBucketMismatch  = "signal_bucket_mismatch"
	FlexiGuardNoFreeSlots     = "no_free_slots"
	FlexiGuardNoVoidDonor     = "no_void_donor"
	FlexiGuardPoolExhausted   = "pool_exhausted"
	FlexiGuardNoSpare         = "no_spare"
	FlexiGuardNoReceiverExtra = "no_receiver_extra"
	FlexiGuardBadMaxPositions = "bad_max_positions"
)

// FlexiGuardIsFailClosed reports whether the guard is an uncertainty
// (operator should see a Warn) rather than a normal "nothing to lend".
func FlexiGuardIsFailClosed(reason string) bool {
	switch reason {
	case FlexiGuardRunDateEmpty, FlexiGuardRunDateMismatch, FlexiGuardUniverseNil,
		FlexiGuardUniverseError, FlexiGuardUniverseRows, FlexiGuardBucketNonCanon,
		FlexiGuardNotInSnapshot, FlexiGuardBucketMismatch, FlexiGuardBadMaxPositions:
		return true
	}
	return false
}

// FlexiPlan is the per-call release decision. It is written verbatim into
// every FLEXI_EVAL payload and every on-mode flexi_grant so the arithmetic
// can be re-derived offline from opp_symbols + manthan_positions + env.
type FlexiPlan struct {
	Mode              FlexiMode            `json:"mode"`
	Base              int                  `json:"base"`
	Total             int                  `json:"total"`
	HeldTotal         int                  `json:"held_total"`
	Free              int                  `json:"free"`
	MinIdle           int                  `json:"min_idle"`
	ReceiverCap       int                  `json:"receiver_cap"`
	Held              map[string]int       `json:"held"`
	Opportunities     map[string]int       `json:"opportunities"`
	OppSymbols        map[string][]string  `json:"opp_symbols"`
	Void              map[string]bool      `json:"void"`
	InfraUnknown      []string             `json:"infra_unknown"`
	Donors            []string             `json:"donors"`
	BaseClaim         map[string]int       `json:"base_claim"`
	SpareBefore       int                  `json:"spare_before"`
	PoolFreeBefore    int                  `json:"pool_free"`
	Extra             map[string]int       `json:"extra"`
	Ceiling           map[string]int       `json:"ceiling"` // only receivers with Ceiling > Base
	BorrowedFrom      map[string][]string  `json:"borrowed_from"`
	Universe          FlexiUniverseSummary `json:"universe"`
	StrategyCreatedAt string               `json:"strategy_created_at"`
	NowIST            string               `json:"now_ist"`
	ShadowSymbols     []string             `json:"shadow_symbols,omitempty"`
}

// InPlay reports whether flexi changes anything for bucket b in this plan.
func (p *FlexiPlan) InPlay(b string) bool {
	return p != nil && p.Ceiling[b] > p.Base
}

// FlexiGrant is attached to an AllocationResult (on-mode only) when the
// entry took a slot ABOVE its bucket's base cap. Persisted as
// manthan_signal_decisions.flexi_grant; never on the trade-signals wire.
type FlexiGrant struct {
	Recipient   string     `json:"recipient"`
	Donors      []string   `json:"donors"`
	Base        int        `json:"base"`
	Ceiling     int        `json:"ceiling"`
	HeldBefore  int        `json:"held_before"`
	SpareBefore int        `json:"spare_before"`
	PoolFree    int        `json:"pool_free"`
	Plan        *FlexiPlan `json:"plan"`
}

// GrantFor builds the audit record for an entry into bucket b under this plan.
func (p *FlexiPlan) GrantFor(b string, caps *CapCheck) FlexiGrant {
	return FlexiGrant{
		Recipient:   b,
		Donors:      append([]string(nil), p.Donors...),
		Base:        p.Base,
		Ceiling:     p.Ceiling[b],
		HeldBefore:  caps.BucketCount[b],
		SpareBefore: p.SpareBefore,
		PoolFree:    p.PoolFreeBefore,
		Plan:        p,
	}
}

// CountOccupied is the number of occupied book slots (see Position.Occupies).
func CountOccupied(positions map[string]*Position) int {
	n := 0
	for _, p := range positions {
		if p.Occupies() {
			n++
		}
	}
	return n
}

// heldBy mirrors the allocator's "already holding" predicate
// (allocator.go): tracked in ANY non-exited state.
func heldBy(positions map[string]*Position, symbol string) bool {
	pos, ok := positions[symbol]
	return ok && pos.State != StateExited
}

// OverlayShadow returns a copy of caps with the dry-run shadow book added
// (entries whose symbol is not held) and the number of entries added.
func OverlayShadow(caps *CapCheck, shadow []ShadowEntry, positions map[string]*Position) (*CapCheck, int, []string) {
	out := caps.Clone()
	seen := map[string]bool{}
	var syms []string
	for _, s := range shadow {
		if s.Symbol == "" || seen[s.Symbol] || heldBy(positions, s.Symbol) {
			continue
		}
		seen[s.Symbol] = true
		out.Add(s.Industry, s.Bucket)
		syms = append(syms, s.Symbol)
	}
	sort.Strings(syms)
	return out, len(syms), syms
}

// BuildFlexiPlan implements §2.1 (VOID / UNIVERSE_PROVEN / INFRA_UNKNOWN)
// and §2.2 (Free / Donors / PoolFree / BaseClaim / Spare / Extra / Ceiling).
//
//	caps      — the cap counters the plan is computed against (live caps in
//	            on-mode; shadow-overlaid caps in dry-run).
//	heldTotal — occupied slots consistent with caps (countActive [+ shadow]).
//	positions — the strategy's book, for held_by_S; shadowSyms are treated
//	            as held too.
//	sigs      — the signals of this Allocate call; every one must be present
//	            in the snapshot with its exact bucket (self-consistency).
//
// Returns (nil, reason) on ANY guard. Never lowers a base cap.
func BuildFlexiPlan(fx *FlexiInput, caps *CapCheck, maxPositions int32, heldTotal int,
	positions map[string]*Position, shadowSyms []string, sigs []ManthanSignal) (*FlexiPlan, string) {

	if fx == nil || !fx.Cfg.Enabled() {
		return nil, FlexiGuardModeOff
	}
	if len(sigs) == 0 {
		return nil, FlexiGuardNoSignals
	}
	if maxPositions <= 0 || caps == nil {
		return nil, FlexiGuardBadMaxPositions
	}
	if fx.Cfg.CutoffReached(fx.NowIST) {
		return nil, FlexiGuardCutoff
	}
	u := fx.Universe
	if u == nil {
		return nil, FlexiGuardUniverseNil
	}
	if u.Err != nil {
		return nil, FlexiGuardUniverseError
	}
	runDate := sigs[0].RunDate
	if runDate == "" {
		return nil, FlexiGuardRunDateEmpty
	}
	for _, s := range sigs {
		if s.RunDate != runDate {
			return nil, FlexiGuardRunDateMismatch
		}
	}
	if u.RunDate != runDate {
		return nil, FlexiGuardRunDateMismatch
	}
	// UNIVERSE_PROVEN: enough manthan_stocks rows for the day …
	if u.StocksRows < fx.Cfg.MinUniverseRows {
		return nil, FlexiGuardUniverseRows
	}
	// … and every signal of this call is in the snapshot with the SAME
	// canonical bucket the cap check will key on.
	bySymbol := make(map[string]FlexiUniverseRow, len(u.Eligible))
	for _, r := range u.Eligible {
		bySymbol[r.Symbol] = r
	}
	for _, s := range sigs {
		if !IsCanonicalBucket(s.MCapBucket) {
			return nil, FlexiGuardBucketNonCanon
		}
		row, ok := bySymbol[s.Symbol]
		if !ok {
			return nil, FlexiGuardNotInSnapshot
		}
		if row.Bucket != s.MCapBucket {
			return nil, FlexiGuardBucketMismatch
		}
	}

	shadow := make(map[string]bool, len(shadowSyms))
	for _, s := range shadowSyms {
		shadow[s] = true
	}

	// OPP(S,X,D): eligible, not held by S, passes the creation gate.
	opp := map[string]int{}
	oppSyms := map[string][]string{}
	for _, r := range u.Eligible {
		if !IsCanonicalBucket(r.Bucket) {
			continue
		}
		if heldBy(positions, r.Symbol) || shadow[r.Symbol] {
			continue
		}
		if !r.FirstSeenAt.IsZero() && !fx.StrategyCreatedAt.IsZero() && r.FirstSeenAt.Before(fx.StrategyCreatedAt) {
			continue
		}
		opp[r.Bucket]++
		oppSyms[r.Bucket] = append(oppSyms[r.Bucket], r.Symbol)
	}
	for b := range oppSyms {
		sort.Strings(oppSyms[b])
	}

	n := int(maxPositions)
	base := caps.MaxPerBucket
	held := map[string]int{}
	for _, b := range CanonicalBuckets {
		held[b] = caps.BucketCount[b]
		if _, ok := opp[b]; !ok {
			opp[b] = 0
		}
	}

	plan := &FlexiPlan{
		Mode:          fx.Cfg.Mode,
		Base:          base,
		Total:         n,
		HeldTotal:     heldTotal,
		MinIdle:       fx.Cfg.MinIdleSlots,
		ReceiverCap:   fx.Cfg.ReceiverCap(n),
		Held:          held,
		Opportunities: opp,
		OppSymbols:    oppSyms,
		Void:          map[string]bool{},
		BaseClaim:     map[string]int{},
		Extra:         map[string]int{},
		Ceiling:       map[string]int{},
		BorrowedFrom:  map[string][]string{},
		Universe: FlexiUniverseSummary{
			StocksRows:     u.StocksRows,
			EligibleRows:   len(u.Eligible),
			StocksByStatus: u.StocksByStatus,
			QueriedAt:      u.QueriedAt.UTC().Format(time.RFC3339),
			QueryMs:        u.QueryMs,
		},
		NowIST:        fx.NowIST.Format(time.RFC3339),
		ShadowSymbols: shadowSyms,
	}
	if !u.SnapshotMaxCreatedAt.IsZero() {
		plan.Universe.SnapshotMaxCreatedAt = u.SnapshotMaxCreatedAt.UTC().Format(time.RFC3339)
	}
	if !fx.StrategyCreatedAt.IsZero() {
		plan.StrategyCreatedAt = fx.StrategyCreatedAt.UTC().Format(time.RFC3339)
	}
	for b, unknown := range u.InfraUnknown {
		if unknown && IsCanonicalBucket(b) {
			plan.InfraUnknown = append(plan.InfraUnknown, b)
		}
	}
	sort.Strings(plan.InfraUnknown)

	// Free = N − ΣHeld − MIN_IDLE

	plan.Free = n - heldTotal - fx.Cfg.MinIdleSlots
	if plan.Free <= 0 {
		return nil, FlexiGuardNoFreeSlots
	}

	// VOID(d) = universe proven ∧ Opp[d]=0 ∧ ¬INFRA_UNKNOWN(d)
	for _, b := range CanonicalBuckets {
		plan.Void[b] = opp[b] == 0 && !u.InfraUnknown[b]
	}
	donorSet := map[string]bool{}
	for _, d := range fx.Cfg.Donors {
		if plan.Void[d] {
			donorSet[d] = true
		}
	}
	for _, b := range CanonicalBuckets { // canonical order for determinism
		if donorSet[b] {
			plan.Donors = append(plan.Donors, b)
		}
	}
	if len(plan.Donors) == 0 {
		return nil, FlexiGuardNoVoidDonor
	}

	// PoolFree = Σ_donors max(0, Base − Held) − Σ_non-donors max(0, Held − Base)
	pool := 0
	for _, b := range CanonicalBuckets {
		if donorSet[b] {
			if room := base - held[b]; room > 0 {
				pool += room
			}
		} else if over := held[b] - base; over > 0 {
			pool -= over // already borrowed (restart-neutral)
		}
	}
	plan.PoolFreeBefore = pool
	if pool <= 0 {
		return nil, FlexiGuardPoolExhausted
	}

	// BaseClaim[b] = min(Opp[b], max(0, Base − Held[b])) for non-void buckets —
	// names b can still take under its OWN base rule are reserved first.
	spare := plan.Free
	for _, b := range CanonicalBuckets {
		if donorSet[b] {
			continue
		}
		room := base - held[b]
		if room < 0 {
			room = 0
		}
		claim := opp[b]
		if room < claim {
			claim = room
		}
		plan.BaseClaim[b] = claim
		spare -= claim
	}
	plan.SpareBefore = spare
	if spare <= 0 {
		return nil, FlexiGuardNoSpare
	}

	// Receivers in priority order.
	for _, r := range fx.Cfg.Priority {
		if donorSet[r] {
			continue
		}
		floor := base
		if held[r] > floor {
			floor = held[r]
		}
		overflow := held[r] + opp[r] - floor
		if overflow < 0 {
			overflow = 0
		}
		extra := overflow
		if spare < extra {
			extra = spare
		}
		if pool < extra {
			extra = pool
		}
		if capRoom := plan.ReceiverCap - floor; capRoom < extra {
			extra = capRoom
		}
		if extra < 0 {
			extra = 0
		}
		plan.Extra[r] = extra
		if ceiling := floor + extra; ceiling > base {
			plan.Ceiling[r] = ceiling
			plan.BorrowedFrom[r] = append([]string(nil), plan.Donors...)
		}
		spare -= extra
		pool -= extra
	}
	if len(plan.Ceiling) == 0 {
		return nil, FlexiGuardNoReceiverExtra
	}
	return plan, ""
}

// ────────────────────────────────────────────────────────────────────
// Evaluation record (FLEXI_EVAL)
// ────────────────────────────────────────────────────────────────────

// FLEXI_EVAL outcomes. WOULD_* are dry-run predictions; GRANTED /
// BLOCKED_FLEXI_CEILING are on-mode facts; BLOCKED_SECTOR, NO_DIFFERENCE and
// WOULD_FAIL_TAIL are shared (in on-mode WOULD_FAIL_TAIL means the ceiling
// admitted the signal but EMA/price/qty rejected it — same as the SKIP row).
const (
	FlexiWouldAllocate           = "WOULD_ALLOCATE"
	FlexiWouldBlockFlexiCeiling  = "WOULD_BLOCK_FLEXI_CEILING"
	FlexiWouldFailTail           = "WOULD_FAIL_TAIL"
	FlexiWouldBlockPortfolioFull = "WOULD_BLOCK_PORTFOLIO_FULL"
	FlexiNoDifference            = "NO_DIFFERENCE"
	FlexiBlockedSector           = "BLOCKED_SECTOR"
	FlexiGranted                 = "GRANTED"
	FlexiBlockedFlexiCeiling     = "BLOCKED_FLEXI_CEILING"
)

// FlexiSim is the dry-run tail simulation (real perCallBase / EMA / price / qty).
type FlexiSim struct {
	EMAAllocPct float64 `json:"ema_alloc_pct"`
	PerCallBase float64 `json:"per_call_base"`
	Quantity    int32   `json:"qty"`
	Invested    float64 `json:"invested"`
	InitialSL   float64 `json:"initial_sl"`
}

// FlexiEval is one signal's evaluation against a plan whose bucket was in play.
type FlexiEval struct {
	Mode        FlexiMode
	Outcome     string
	Symbol      string
	RunDate     string
	Bucket      string
	Industry    string
	IndexName   string
	ISIN        string
	LatestPrice float64
	// BaseOutcome is what today's base caps decided for the same signal
	// ("" = allocatable under base, else the skip reason) — the dry-run
	// control the Phase-1 evidence compares against.
	BaseOutcome string
	EvalHeld    int // receiver bucket count the eval saw (incl. shadow in dry-run)
	EvalLimit   int // ceiling the eval applied
	TailReason  string
	Sim         *FlexiSim
	Plan        *FlexiPlan
	Config      map[string]any
}

// Summary is the one-line rejection_reason text stored with the row.
func (e FlexiEval) Summary() string {
	donors := []string(nil)
	if e.Plan != nil {
		donors = e.Plan.Donors
	}
	s := fmt.Sprintf("FLEXI_EVAL %s %s %s held %d/ceiling %d (base %d, donors %v)",
		e.Mode, e.Outcome, e.Bucket, e.EvalHeld, e.EvalLimit, planBase(e.Plan), donors)
	if e.TailReason != "" {
		s += ": " + e.TailReason
	}
	return s
}

func planBase(p *FlexiPlan) int {
	if p == nil {
		return 0
	}
	return p.Base
}
