// nav-backfill — one-off, re-runnable reconstruction of each deployment's
// TRUE daily NAV curve from deploy date to yesterday.
//
// WHY: strategy_nav_daily only accrues from 2026-08-20 (the snapshot job's
// birth), but every input needed for the PAST is real and available:
//   - the lot ledger (positions_db): entry/exit time, price, qty, realized;
//   - daily closes per symbol: Yahoo Finance (SYMBOL.NS) — the same source
//     scripts/sync_nifty_benchmark.sh already uses for ^NSEI.
//
// NAV(d) = deployed + realized_cum(≤d) + Σ_open_lots (close_d − entry)×qty.
//
// SAFETY: writes ONLY dates strictly before today (the live snapshot job
// owns today) via the same idempotent upsert. Re-run any time.
//
// USAGE (on the box): go run ./services/api-gateway/cmd/nav-backfill/
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"time"

	_ "github.com/lib/pq"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

func dsn(db string) string {
	host := envOr("PGHOST", "localhost")
	port := envOr("PGPORT", "5442")
	return fmt.Sprintf("host=%s port=%s user=postgres password=postgres dbname=%s sslmode=disable", host, port, db)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type lot struct {
	symbol     string
	qty        float64
	entryPrice float64
	entryDate  string // IST yyyy-mm-dd
	exitDate   string // "" while open
	realized   float64
}

func main() {
	ist, _ := time.LoadLocation("Asia/Kolkata")
	ctx := context.Background()
	tradingDB := mustOpen(dsn("trading_db"))
	positionsDB := mustOpen(dsn("positions_db"))
	marketDB := mustOpen(dsn("stockk_market"))
	nav := livealgos.NewNAVStore(marketDB)
	today := time.Now().In(ist).Format("2006-01-02")

	deps, err := tradingDB.QueryContext(ctx, `
		SELECT s.strategy_id::text, s.user_id, COALESCE(tc.total_capital,0), s.created_at
		FROM strategies s LEFT JOIN trade_configs tc ON tc.strategy_id = s.strategy_id
		WHERE s.strategy_type='MANTHAN' AND s.active AND s.deleted_at IS NULL`)
	must(err)
	defer deps.Close()

	// Trading-day calendar from the index itself.
	calendar := yahooCloses("^NSEI", ist)
	var days []string
	for d := range calendar {
		days = append(days, d)
	}
	sort.Strings(days)

	closeCache := map[string]map[string]float64{}
	for deps.Next() {
		var strategyID, userID string
		var capital float64
		var createdAt time.Time
		must(deps.Scan(&strategyID, &userID, &capital, &createdAt))
		deployDate := createdAt.In(ist).Format("2006-01-02")

		rows, err := positionsDB.QueryContext(ctx, `
			SELECT symbol, quantity, entry_price, entry_time, exit_time, COALESCE(realized_pnl, 0)
			FROM positions WHERE strategy_id::text = $1 AND origin = 'MANTHAN'`, strategyID)
		must(err)
		var lots []lot
		for rows.Next() {
			var l lot
			var entryT time.Time
			var exitT sql.NullTime
			must(rows.Scan(&l.symbol, &l.qty, &l.entryPrice, &entryT, &exitT, &l.realized))
			l.entryDate = entryT.In(ist).Format("2006-01-02")
			if exitT.Valid {
				l.exitDate = exitT.Time.In(ist).Format("2006-01-02")
			}
			lots = append(lots, l)
		}
		rows.Close()

		for _, l := range lots {
			if _, ok := closeCache[l.symbol]; !ok {
				closeCache[l.symbol] = yahooCloses(l.symbol+".NS", ist)
			}
		}

		wrote := 0
		fmt.Printf("── %s (%s) deployed %s capital %.0f ──\n", userID, strategyID[:8], deployDate, capital)
		for _, d := range days {
			if d < deployDate || d >= today {
				continue
			}
			var realized, unrealized float64
			var open int
			for _, l := range lots {
				if l.entryDate > d {
					continue
				}
				if l.exitDate != "" && l.exitDate <= d {
					realized += l.realized
					continue
				}
				open++
				unrealized += (closeOn(closeCache[l.symbol], l.symbol, d, l.entryPrice) - l.entryPrice) * l.qty
			}
			net := realized + unrealized
			pct := 0.0
			if capital > 0 {
				pct = net / capital * 100
			}
			date, _ := time.ParseInLocation("2006-01-02", d, time.UTC)
			must(nav.Upsert(ctx, livealgos.NAVRow{
				StrategyID: strategyID, UserID: userID, Date: date,
				DeployedCapital: int64(capital), NetPnLAmount: int64(net),
				NetPnLPct:      round2(pct),
				RealizedAmount: int64(realized), UnrealizedAmount: int64(unrealized),
				OpenPositions: open,
			}))
			wrote++
			fmt.Printf("  %s  net=%9.0f  pct=%6.2f%%  open=%d\n", d, net, pct, open)
		}
		fmt.Printf("  → %d day(s) backfilled\n", wrote)
	}
}

// closeOn: the symbol's close on d, else last known close before d, else entry.
func closeOn(closes map[string]float64, sym, d string, fallback float64) float64 {
	if c, ok := closes[d]; ok && c > 0 {
		return c
	}
	var best string
	for cd := range closes {
		if cd < d && cd > best {
			best = cd
		}
	}
	if best != "" && closes[best] > 0 {
		return closes[best]
	}
	fmt.Printf("  WARN: no close for %s on/before %s — using entry price\n", sym, d)
	return fallback
}

func yahooCloses(symbol string, ist *time.Location) map[string]float64 {
	u := fmt.Sprintf("https://query1.finance.yahoo.com/v8/finance/chart/%s?range=1mo&interval=1d", url.PathEscape(symbol))
	req, _ := http.NewRequest("GET", u, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := http.DefaultClient.Do(req)
	must(err)
	defer resp.Body.Close()
	var doc struct {
		Chart struct {
			Result []struct {
				Timestamp  []int64 `json:"timestamp"`
				Indicators struct {
					Quote []struct {
						Close []*float64 `json:"close"`
					} `json:"quote"`
				} `json:"indicators"`
			} `json:"result"`
		} `json:"chart"`
	}
	must(json.NewDecoder(resp.Body).Decode(&doc))
	out := map[string]float64{}
	if len(doc.Chart.Result) == 0 {
		fmt.Printf("  WARN: yahoo returned nothing for %s\n", symbol)
		return out
	}
	r := doc.Chart.Result[0]
	if len(r.Indicators.Quote) == 0 {
		return out
	}
	for i, ts := range r.Timestamp {
		if i < len(r.Indicators.Quote[0].Close) && r.Indicators.Quote[0].Close[i] != nil {
			out[time.Unix(ts, 0).In(ist).Format("2006-01-02")] = *r.Indicators.Quote[0].Close[i]
		}
	}
	return out
}

func round2(f float64) float64 {
	return float64(int(f*100+map[bool]float64{true: 0.5, false: -0.5}[f >= 0])) / 100
}

func mustOpen(d string) *sql.DB {
	db, err := sql.Open("postgres", d)
	must(err)
	must(db.Ping())
	return db
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
