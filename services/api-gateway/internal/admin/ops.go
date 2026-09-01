package admin

// M9 — risk & limits. M10 — system health. M11 — compliance exports.
//
// 9.1 Caps utilization: per strategy, sector slots (ceil(max_positions ×
//     0.25) — the allocator's exact formula) and mcap-bucket slots
//     (ceil(× 0.50)), INCLUDING in-flight reservations (PENDING_ENTRY /
//     PARTIAL_ACTIVE occupy caps by design); aggregate exposure vs
//     configured capital; live margin headroom from the broker.
// 9.2 Allocation drivers: the EMA allocation map (redis
//     manthan:ema:allocations, refreshed daily by data-ingestion) — the
//     sizing modulator. Risk-management limits: declared not_wired.
// 10.1 Infra board: PM2 process table, DB ping latencies, LTP feed
//     health, trade-execution reachability, reconciler activity, disk.
//     Kafka consumer lag + per-user WSS state: declared not_wired.
// 11.1 Exports: date-range CSVs in the shapes hand-built for the NSE
//     submission — orders register, per-order event trail, admin-action
//     report. One click, consistent forever.

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"syscall"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// ── M9 risk & limits ────────────────────────────────────────────────────

// fundLimiter is the one broker call 9.1 needs.
type fundLimiter interface {
	GetFundLimit(ctx context.Context, auth *indiraClient.AuthContext) (*indiraClient.FundLimit, error)
}

// emaGetter fetches the raw EMA allocation JSON ("" if unavailable).
type emaGetter func(ctx context.Context) (string, error)

type CapSlot struct {
	Name  string `json:"name"`
	Used  int    `json:"used"`  // incl. in-flight reservations
	Limit int    `json:"limit"` // ceil(max_positions × pct)
	Full  bool   `json:"full"`
}

type StrategyCaps struct {
	StrategyID    string    `json:"strategy_id"`
	UserID        string    `json:"user_id"`
	MaxPositions  int       `json:"max_positions"`
	TotalCapital  float64   `json:"total_capital"`
	OpenPositions int       `json:"open_positions"` // ACTIVE + in-flight
	InFlight      int       `json:"in_flight"`
	Invested      float64   `json:"invested"`
	ExposurePct   float64   `json:"exposure_pct"` // invested / total_capital
	Sectors       []CapSlot `json:"sectors"`      // 25% rule
	McapBuckets   []CapSlot `json:"mcap_buckets"` // 50% rule

	MarginLeg       string  `json:"margin_leg"` // OK | AUTH_EXPIRED | ...
	AvailableCash   float64 `json:"available_cash,omitempty"`
	AvailableMargin float64 `json:"available_margin,omitempty"`
}

// RiskStore assembles 9.1/9.2.
type RiskStore struct {
	fleet  *FleetStore
	creds  credentialsFetcher
	broker fundLimiter
	ema    emaGetter
}

func NewRiskStore(fleet *FleetStore, creds credentialsFetcher, broker fundLimiter, ema emaGetter) *RiskStore {
	return &RiskStore{fleet: fleet, creds: creds, broker: broker, ema: ema}
}

// Caps computes 9.1 for every live strategy.
func (rs *RiskStore) Caps(ctx context.Context) ([]StrategyCaps, error) {
	rows, err := rs.fleet.tradingDB.QueryContext(ctx, `
		SELECT s.strategy_id, s.user_id,
		       COALESCE(tc.max_positions, 25), COALESCE(tc.total_capital, 0)
		  FROM strategies s
		  LEFT JOIN LATERAL (
		      SELECT max_positions, total_capital FROM trade_configs
		       WHERE strategy_id = s.strategy_id ORDER BY created_at DESC LIMIT 1
		  ) tc ON true
		 WHERE s.deleted_at IS NULL AND s.active = true AND s.strategy_type = 'MANTHAN'`)
	if err != nil {
		return nil, fmt.Errorf("caps strategies: %w", err)
	}
	var out []StrategyCaps
	for rows.Next() {
		var sc StrategyCaps
		if err := rows.Scan(&sc.StrategyID, &sc.UserID, &sc.MaxPositions, &sc.TotalCapital); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, sc)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range out {
		sc := &out[i]
		if err := rs.fillCaps(ctx, sc); err != nil {
			return nil, err
		}
		rs.fillMargin(ctx, sc)
	}
	if out == nil {
		out = []StrategyCaps{}
	}
	return out, nil
}

func (rs *RiskStore) fillCaps(ctx context.Context, sc *StrategyCaps) error {
	rows, err := rs.fleet.tradingDB.QueryContext(ctx, `
		SELECT COALESCE(industry,'?'), COALESCE(mcap_bucket,'?'), status, COALESCE(invested_amt,0)
		  FROM manthan_positions
		 WHERE strategy_id = $1::uuid AND status IN ('ACTIVE','PENDING_ENTRY','PARTIAL_ACTIVE')`,
		sc.StrategyID)
	if err != nil {
		return fmt.Errorf("caps positions: %w", err)
	}
	defer rows.Close()
	sectors, buckets := map[string]int{}, map[string]int{}
	for rows.Next() {
		var industry, bucket, status string
		var invested float64
		if err := rows.Scan(&industry, &bucket, &status, &invested); err != nil {
			return err
		}
		sc.OpenPositions++
		if status != "ACTIVE" {
			sc.InFlight++
		}
		sc.Invested += invested
		sectors[industry]++
		buckets[bucket]++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if sc.TotalCapital > 0 {
		sc.ExposurePct = sc.Invested / sc.TotalCapital * 100
	}
	sectorLimit := int(math.Ceil(float64(sc.MaxPositions) * 0.25))
	bucketLimit := int(math.Ceil(float64(sc.MaxPositions) * 0.50))
	for name, used := range sectors {
		sc.Sectors = append(sc.Sectors, CapSlot{Name: name, Used: used, Limit: sectorLimit, Full: used >= sectorLimit})
	}
	for name, used := range buckets {
		sc.McapBuckets = append(sc.McapBuckets, CapSlot{Name: name, Used: used, Limit: bucketLimit, Full: used >= bucketLimit})
	}
	sort.Slice(sc.Sectors, func(i, j int) bool { return sc.Sectors[i].Used > sc.Sectors[j].Used })
	sort.Slice(sc.McapBuckets, func(i, j int) bool { return sc.McapBuckets[i].Used > sc.McapBuckets[j].Used })
	if sc.Sectors == nil {
		sc.Sectors = []CapSlot{}
	}
	if sc.McapBuckets == nil {
		sc.McapBuckets = []CapSlot{}
	}
	return nil
}

func (rs *RiskStore) fillMargin(ctx context.Context, sc *StrategyCaps) {
	auth, verdict, _ := fetchAuthFor(ctx, rs.creds, sc.UserID)
	if verdict != "" {
		sc.MarginLeg = verdict
		return
	}
	fl, err := rs.broker.GetFundLimit(ctx, auth)
	if err != nil {
		if isAuthExpired(err) {
			sc.MarginLeg = "AUTH_EXPIRED"
		} else {
			sc.MarginLeg = "ERROR"
		}
		return
	}
	sc.MarginLeg = "OK"
	sc.AvailableCash, sc.AvailableMargin = fl.AvailableCash, fl.AvailableMargin
}

// Drivers returns 9.2: the EMA allocation map + declared gaps.
func (rs *RiskStore) Drivers(ctx context.Context) (map[string]any, error) {
	out := map[string]any{
		"not_wired": []string{"risk-management profile limits (read integration pending)"},
	}
	if rs.ema == nil {
		out["ema_allocations"] = nil
		out["ema_status"] = "UNWIRED"
		return out, nil
	}
	raw, err := rs.ema(ctx)
	if err != nil || raw == "" {
		out["ema_allocations"] = nil
		out["ema_status"] = "UNAVAILABLE"
		if err != nil {
			out["ema_error"] = err.Error()
		}
		return out, nil
	}
	var ema map[string]float64
	if jerr := json.Unmarshal([]byte(raw), &ema); jerr != nil {
		out["ema_allocations"] = nil
		out["ema_status"] = "UNPARSEABLE"
		return out, nil
	}
	out["ema_allocations"] = ema
	out["ema_status"] = "OK"
	out["note"] = "per-index EMA target × per_call sizes every position (refreshed daily post-market by data-ingestion)"
	return out, nil
}

// ── M10 system health ───────────────────────────────────────────────────

type PM2Proc struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	UptimeS  int64  `json:"uptime_s"`
	Restarts int    `json:"restarts"`
}

type InfraBoard struct {
	Services    []PM2Proc         `json:"services,omitempty"`
	PM2Error    string            `json:"pm2_error,omitempty"`
	DBPings     map[string]string `json:"db_pings"` // name → "ok 3ms" | error
	LTPHealthy  *bool             `json:"ltp_healthy,omitempty"`
	TradeExec   string            `json:"trade_execution"` // healthz verdict
	ReconFixed  int               `json:"reconciler_fixed_24h"`
	DiskUsedPct float64           `json:"disk_used_pct"`
	NotWired    []string          `json:"not_wired"`
	GeneratedAt time.Time         `json:"generated_at"`
}

type ltpHealth interface{ IsHealthy() bool }

// OpsStore assembles 10.1.
type OpsStore struct {
	fleet        *FleetStore
	teMetricsURL string
	ltp          ltpHealth
	run          cmdRunner
	httpc        *http.Client
}

func NewOpsStore(fleet *FleetStore, teMetricsURL string, ltp ltpHealth) *OpsStore {
	return &OpsStore{fleet: fleet, teMetricsURL: strings.TrimRight(teMetricsURL, "/"),
		ltp: ltp, run: execRunner, httpc: &http.Client{Timeout: 5 * time.Second}}
}

func (o *OpsStore) Board(ctx context.Context) *InfraBoard {
	b := &InfraBoard{DBPings: map[string]string{}, GeneratedAt: time.Now(),
		NotWired: []string{"kafka consumer lag", "per-user broker WSS state"}}

	// PM2 process table.
	if out, err := o.run(ctx, "", "pm2", "jlist"); err != nil {
		b.PM2Error = truncate(err.Error(), 120)
	} else {
		var procs []struct {
			Name   string `json:"name"`
			PM2Env struct {
				Status    string `json:"status"`
				PMUptime  int64  `json:"pm_uptime"`
				RestartTs int    `json:"restart_time"`
			} `json:"pm2_env"`
		}
		if jerr := json.Unmarshal(out, &procs); jerr != nil {
			b.PM2Error = "jlist parse: " + truncate(jerr.Error(), 100)
		} else {
			for _, pr := range procs {
				up := int64(0)
				if pr.PM2Env.PMUptime > 0 {
					up = (time.Now().UnixMilli() - pr.PM2Env.PMUptime) / 1000
				}
				b.Services = append(b.Services, PM2Proc{Name: pr.Name, Status: pr.PM2Env.Status,
					UptimeS: up, Restarts: pr.PM2Env.RestartTs})
			}
		}
	}

	// DB pings with latency.
	for name, db := range map[string]*sql.DB{
		"trading_db": o.fleet.tradingDB, "execution_db": o.fleet.execDB, "positions_db": o.fleet.posDB,
	} {
		start := time.Now()
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if err := db.PingContext(pctx); err != nil {
			b.DBPings[name] = "FAIL: " + truncate(err.Error(), 80)
		} else {
			b.DBPings[name] = fmt.Sprintf("ok %dms", time.Since(start).Milliseconds())
		}
		cancel()
	}

	// LTP feed (the silent-tunnel failure mode).
	if o.ltp != nil {
		h := o.ltp.IsHealthy()
		b.LTPHealthy = &h
	}

	// trade-execution reachability.
	if req, err := http.NewRequestWithContext(ctx, "GET", o.teMetricsURL+"/healthz", nil); err == nil {
		if resp, herr := o.httpc.Do(req); herr != nil {
			b.TradeExec = "UNREACHABLE: " + truncate(herr.Error(), 80)
		} else {
			resp.Body.Close()
			b.TradeExec = fmt.Sprintf("ok (%d)", resp.StatusCode)
		}
	}

	// Reconciler activity (same predicate the attention queue uses).
	_ = o.fleet.execDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM manthan_order_events
		 WHERE event_type = 'RECONCILER_FIXED' AND created_at >= now() - interval '24 hours'`).
		Scan(&b.ReconFixed)

	// Disk.
	var st syscall.Statfs_t
	if err := syscall.Statfs("/", &st); err == nil && st.Blocks > 0 {
		b.DiskUsedPct = 100 * (1 - float64(st.Bavail)/float64(st.Blocks))
	}
	return b
}

// ── M11 compliance exports ──────────────────────────────────────────────

// ExportStore streams date-range CSVs in the NSE-submission shapes.
type ExportStore struct {
	fleet *FleetStore
	admin *Store // admin_audit
}

func NewExportStore(fleet *FleetStore, admin *Store) *ExportStore {
	return &ExportStore{fleet: fleet, admin: admin}
}

// parseRange reads from/to (YYYY-MM-DD, inclusive); defaults last 7 days.
func parseRange(fromS, toS string) (time.Time, time.Time, error) {
	to := time.Now()
	from := to.AddDate(0, 0, -7)
	var err error
	if fromS != "" {
		if from, err = time.Parse("2006-01-02", fromS); err != nil {
			return from, to, fmt.Errorf("bad from date %q (want YYYY-MM-DD)", fromS)
		}
	}
	if toS != "" {
		t, terr := time.Parse("2006-01-02", toS)
		if terr != nil {
			return from, to, fmt.Errorf("bad to date %q (want YYYY-MM-DD)", toS)
		}
		to = t.AddDate(0, 0, 1) // inclusive
	}
	if !to.After(from) {
		return from, to, fmt.Errorf("empty range")
	}
	return from, to, nil
}

// streamCSV writes query results as CSV — header from columns, all values
// rendered as text; the exact shape of the hand-built NSE dumps.
func streamCSV(w http.ResponseWriter, filename string, rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	cw := csv.NewWriter(w)
	if err := cw.Write(cols); err != nil {
		return err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	rec := make([]string, len(cols))
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		for i, v := range vals {
			switch t := v.(type) {
			case nil:
				rec[i] = ""
			case time.Time:
				rec[i] = t.UTC().Format(time.RFC3339)
			case []byte:
				rec[i] = string(t)
			default:
				rec[i] = fmt.Sprint(t)
			}
		}
		if err := cw.Write(rec); err != nil {
			return err
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}
	return rows.Err()
}

// OrdersCSV — the order register.
func (x *ExportStore) OrdersCSV(ctx context.Context, w http.ResponseWriter, from, to time.Time) error {
	rows, err := x.fleet.execDB.QueryContext(ctx, `
		SELECT id, signal_id, strategy_id, user_id, symbol, isin, exchange,
		       order_type, order_side, product_type, qty, filled_qty,
		       limit_price, trigger_price, avg_fill_price,
		       broker_order_id, broker_status, status, retry_count, last_error,
		       exchange_error_code, exchange_error_tag, reject_category,
		       trade_date, created_at, placed_at, filled_at, cancelled_at, updated_at
		  FROM manthan_orders
		 WHERE created_at >= $1 AND created_at < $2
		 ORDER BY id`, from, to)
	if err != nil {
		return err
	}
	defer rows.Close()
	return streamCSV(w, fmt.Sprintf("orders_%s_%s.csv", from.Format("20060102"), to.Format("20060102")), rows)
}

// EventsCSV — the per-order event trail, with order context joined in.
func (x *ExportStore) EventsCSV(ctx context.Context, w http.ResponseWriter, from, to time.Time, orderID string) error {
	q := `
		SELECT e.id, e.order_id, o.symbol, o.user_id, o.order_type,
		       e.event_type, e.old_status, e.new_status, e.broker_status,
		       e.price, e.qty, e.detail, e.created_at
		  FROM manthan_order_events e
		  LEFT JOIN manthan_orders o ON o.id = e.order_id
		 WHERE e.created_at >= $1 AND e.created_at < $2`
	args := []any{from, to}
	if orderID != "" {
		q += ` AND e.order_id = $3`
		args = append(args, orderID)
	}
	q += ` ORDER BY e.id`
	rows, err := x.fleet.execDB.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return streamCSV(w, fmt.Sprintf("order_events_%s_%s.csv", from.Format("20060102"), to.Format("20060102")), rows)
}

// AdminActionsCSV — the admin-action report from the append-only audit.
func (x *ExportStore) AdminActionsCSV(ctx context.Context, w http.ResponseWriter, from, to time.Time) error {
	rows, err := x.admin.db.QueryContext(ctx, `
		SELECT id, admin_id, action, tier, target_user, target_ref, params,
		       result, detail, self_action, ip, created_at
		  FROM admin_audit
		 WHERE created_at >= $1 AND created_at < $2
		 ORDER BY id`, from, to)
	if err != nil {
		return err
	}
	defer rows.Close()
	return streamCSV(w, fmt.Sprintf("admin_actions_%s_%s.csv", from.Format("20060102"), to.Format("20060102")), rows)
}
