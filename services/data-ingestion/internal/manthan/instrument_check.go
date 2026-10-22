package manthan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// ─── Instrument gate against the LIVE price feed (2026-10-22) ────────────────
//
// Every downstream step — allocation price, entry order, stop, exit — resolves
// a stock through the external price Redis: isin:<ISIN> → exchange token →
// market:nse:<token> → live price. On 2026-10-22 exits failed for weeks
// because that chain was broken for stocks already in the book. This gate
// makes the chain a precondition for a signal to EXIST: an eligible sheet row
// whose ISIN is unknown to the feed, disagrees with the feed's own
// symbol→ISIN key, or has no live price row is downgraded to FILTER_REJECTED
// with the exact reason, so the operator fixes the sheet before anything is
// bought. A feed outage never silently drops the day's signals: transport
// errors are reported as Transient and the publisher lets the row through
// (trade-execution re-checks freshness before it buys).

// InstrumentVerdict is the outcome of one instrument check.
type InstrumentVerdict struct {
	OK        bool
	Token     string // live NSE exchange token when OK
	Reason    string // why the instrument is not tradeable (when !OK)
	Transient bool   // the check itself failed (Redis unreachable) — not a verdict on the stock
}

// InstrumentChecker verifies that a (symbol, ISIN) pair is tradeable through
// the live price feed.
type InstrumentChecker interface {
	Check(ctx context.Context, symbol, isin string) InstrumentVerdict
}

// priceRowMaxAge — a market:nse row older than this is treated as dead (the
// feed stopped publishing that token). Five days covers a long weekend plus
// a holiday; BODALCHEM's dead row was 23 days old.
const priceRowMaxAge = 5 * 24 * time.Hour

// RedisInstrumentChecker checks against the external price Redis. The GET is
// injectable so the logic is unit-tested without a Redis server.
type RedisInstrumentChecker struct {
	get    func(ctx context.Context, key string) (string, error)
	now    func() time.Time
	logger *zap.Logger
}

// NewRedisInstrumentChecker connects to the external price Redis (DB 0).
func NewRedisInstrumentChecker(addr, password string, logger *zap.Logger) *RedisInstrumentChecker {
	rdb := goredis.NewClient(&goredis.Options{Addr: addr, Password: password, DB: 0,
		DialTimeout: 3 * time.Second, ReadTimeout: 2 * time.Second})
	return &RedisInstrumentChecker{
		get:    func(ctx context.Context, key string) (string, error) { return rdb.Get(ctx, key).Result() },
		now:    time.Now,
		logger: logger,
	}
}

// newInstrumentCheckerWithGetter is the test seam.
func newInstrumentCheckerWithGetter(get func(ctx context.Context, key string) (string, error), now func() time.Time, logger *zap.Logger) *RedisInstrumentChecker {
	return &RedisInstrumentChecker{get: get, now: now, logger: logger}
}

func (c *RedisInstrumentChecker) Check(ctx context.Context, symbol, isin string) InstrumentVerdict {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	isin = strings.ToUpper(strings.TrimSpace(isin))
	if len(isin) != 12 || !strings.HasPrefix(isin, "IN") {
		return InstrumentVerdict{Reason: fmt.Sprintf("ISIN %q malformed (expected 12 chars starting IN) — fix the sheet row", isin)}
	}

	// 1. ISIN → token (the live mapping).
	raw, err := c.get(ctx, "isin:"+isin)
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return InstrumentVerdict{Reason: fmt.Sprintf("ISIN %s not in the price feed (isin: key missing) — check the ISIN in the sheet", isin)}
		}
		return InstrumentVerdict{Transient: true, Reason: "price feed unreachable: " + err.Error()}
	}
	var m struct {
		NSECode string `json:"nsecode"`
	}
	if json.Unmarshal([]byte(raw), &m) != nil || strings.TrimSpace(m.NSECode) == "" {
		return InstrumentVerdict{Reason: fmt.Sprintf("ISIN %s has no NSE token in the price feed", isin)}
	}
	token := strings.TrimSpace(m.NSECode)

	// 2. The feed's own symbol → ISIN must agree with the sheet (catches a
	//    wrong ISIN typed against the right name, or vice versa).
	if symbol != "" {
		if sraw, serr := c.get(ctx, "symbol:"+symbol); serr == nil {
			var s struct {
				ISIN string `json:"isin"`
			}
			if json.Unmarshal([]byte(sraw), &s) == nil {
				if fi := strings.ToUpper(strings.TrimSpace(s.ISIN)); fi != "" && fi != isin {
					return InstrumentVerdict{Reason: fmt.Sprintf("symbol/ISIN mismatch: sheet says %s, price feed says %s for %s — fix the sheet row", isin, fi, symbol)}
				}
			}
		} else if !errors.Is(serr, goredis.Nil) {
			return InstrumentVerdict{Transient: true, Reason: "price feed unreachable: " + serr.Error()}
		}
	}

	// 3. A live price row must exist under that token and not be dead.
	praw, perr := c.get(ctx, "market:nse:"+token)
	if perr != nil {
		if errors.Is(perr, goredis.Nil) {
			return InstrumentVerdict{Reason: fmt.Sprintf("no price row in the feed for %s (token %s) — stock not being published", symbol, token)}
		}
		return InstrumentVerdict{Transient: true, Reason: "price feed unreachable: " + perr.Error()}
	}
	var row struct {
		LTP         float64 `json:"ltp"`
		TimestampMs int64   `json:"timestamp"`
		LastUpdated string  `json:"last_updated"`
	}
	if json.Unmarshal([]byte(praw), &row) != nil || row.LTP <= 0 {
		return InstrumentVerdict{Reason: fmt.Sprintf("price row for %s (token %s) carries no LTP", symbol, token)}
	}
	var asOf time.Time
	switch {
	case row.TimestampMs > 1e12:
		asOf = time.UnixMilli(row.TimestampMs)
	case row.TimestampMs > 1e9:
		asOf = time.Unix(row.TimestampMs, 0)
	case row.LastUpdated != "":
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
			if t, e := time.Parse(layout, strings.TrimSpace(row.LastUpdated)); e == nil {
				asOf = t
				break
			}
		}
	}
	if !asOf.IsZero() && c.now().Sub(asOf) > priceRowMaxAge {
		return InstrumentVerdict{Reason: fmt.Sprintf("price row for %s (token %s) is dead — last update %s", symbol, token, asOf.Format("2006-01-02 15:04"))}
	}
	return InstrumentVerdict{OK: true, Token: token}
}
