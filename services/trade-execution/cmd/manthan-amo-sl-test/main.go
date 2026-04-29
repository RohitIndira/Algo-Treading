// manthan-amo-sl-test — one-shot CLI to validate whether Indira's AMO endpoint
// accepts SL-trigger orders that survive into the next session as live SL.
//
// USAGE:
//
//   # Place AMO+SL orders for every long position+holding the user owns:
//   JWT='eyJhbG...' APP_ID='1e7f...' USER_ID='ND03920' \
//     go run ./services/trade-execution/cmd/manthan-amo-sl-test/
//
//   # Tomorrow at 9:16 IST, verify they materialised:
//   JWT='...' APP_ID='...' USER_ID='ND03920' \
//     go run ./services/trade-execution/cmd/manthan-amo-sl-test/ --verify
//
//   # Cancel everything we placed (e.g. before market open if the test was conclusive):
//   JWT='...' APP_ID='...' USER_ID='ND03920' \
//     go run ./services/trade-execution/cmd/manthan-amo-sl-test/ --cancel
//
// The CLI fetches positions+holdings, places AMO + SL-Limit SELL at LTP*(1-buffer)
// for each net-long DELIVERY/CNC line, and writes a results JSON to /tmp.
// Any symbol that already has a live SL SELL is skipped to avoid duplication.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/manthan"
)

const minLTPForTest = 10.0 // skip penny stocks — tick / DPR issues

const (
	defaultBufferPct = 5.0
	defaultTickSize  = 0.05
)

type TestResult struct {
	Symbol        string  `json:"symbol"`
	DispSym       string  `json:"disp_sym"`
	Exchange      string  `json:"exchange"`
	ExcToken      string  `json:"exc_token"`
	Source        string  `json:"source"`
	Qty           int     `json:"qty"`
	LTP           float64 `json:"ltp"`
	TriggerPrice  float64 `json:"trigger_price"`
	LimitPrice    float64 `json:"limit_price"`
	BrokerOrderID string  `json:"broker_order_id,omitempty"`
	OrdStatus     string  `json:"ord_status,omitempty"`
	RejReason     string  `json:"rej_reason,omitempty"`
	Error         string  `json:"error,omitempty"`
	Skipped       bool    `json:"skipped,omitempty"`
	SkipReason    string  `json:"skip_reason,omitempty"`
}

type ResultsFile struct {
	UserID      string       `json:"user_id"`
	SubmittedAt time.Time    `json:"submitted_at"`
	BufferPct   float64      `json:"buffer_pct"`
	Tests       []TestResult `json:"tests"`
}

func main() {
	verify := flag.Bool("verify", false, "fetch order book and report status of saved test orders")
	cancel := flag.Bool("cancel", false, "cancel all orders we placed (reads from --out)")
	bufferPct := flag.Float64("buffer-pct", defaultBufferPct, "trigger price buffer below LTP (percent)")
	dryRun := flag.Bool("dry-run", false, "print plan, don't submit")
	baseURL := flag.String("base-url", "https://livemiddleware.indiratrade.com", "broker base URL")
	outFile := flag.String("out", "", "output JSON path (default /tmp/amo-sl-test-USERID-YYYYMMDD.json)")
	symbolsCSV := flag.String("symbols", "", "comma-separated whitelist of disp symbols to include (default: all)")
	flag.Parse()

	includeSet := map[string]bool{}
	if *symbolsCSV != "" {
		for _, s := range strings.Split(*symbolsCSV, ",") {
			s = strings.TrimSpace(strings.ToUpper(s))
			if s != "" {
				includeSet[s] = true
			}
		}
	}

	jwt := strings.TrimSpace(os.Getenv("JWT"))
	appID := strings.TrimSpace(os.Getenv("APP_ID"))
	userID := strings.TrimSpace(os.Getenv("USER_ID"))
	source := os.Getenv("SOURCE")
	if source == "" {
		source = "WEB"
	}

	if jwt == "" || appID == "" || userID == "" {
		fmt.Fprintln(os.Stderr, "ERROR: env vars JWT, APP_ID, USER_ID are required")
		os.Exit(2)
	}

	if *outFile == "" {
		*outFile = fmt.Sprintf("/tmp/amo-sl-test-%s-%s.json", userID, time.Now().Format("20060102"))
	}

	auth := &indira.AuthContext{
		BearerToken: jwt,
		AppId:       appID,
		UserId:      userID,
		Source:      source,
	}

	client := indira.NewClient(indira.Config{BaseURL: *baseURL})
	ctx, cancelCtx := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelCtx()

	// For placement we route through the production BrokerAdapter so DPR
	// clamping + 50bps overnight-safety buffer apply identically to what the
	// live replayer will do. Requires EXT_REDIS_ADDR (vendor market-data
	// feed) — verify/cancel paths don't need it.
	var adapter *manthan.BrokerAdapter
	if !*verify && !*cancel {
		extAddr := strings.TrimSpace(os.Getenv("EXT_REDIS_ADDR"))
		extPass := os.Getenv("EXT_REDIS_PASSWORD")
		if extAddr == "" {
			fmt.Fprintln(os.Stderr,
				"ERROR: EXT_REDIS_ADDR (and EXT_REDIS_PASSWORD if set) required for placement —"+
					" the test must use the same DPR data path as production.")
			os.Exit(2)
		}
		extRedis := redis.NewClient(&redis.Options{Addr: extAddr, Password: extPass})
		if err := extRedis.Ping(ctx).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: external Redis ping failed: %v\n", err)
			os.Exit(2)
		}
		zlog, _ := zap.NewDevelopment()
		adapter = manthan.NewBrokerAdapter(client, extRedis, zlog)
	}

	switch {
	case *verify:
		runVerify(ctx, client, auth, *outFile)
	case *cancel:
		runCancel(ctx, client, auth, *outFile)
	default:
		runPlace(ctx, client, auth, adapter, userID, *bufferPct, *dryRun, *outFile, includeSet)
	}
}

// ───────────────────────────────────────── place ─────────────────────────────────────────

func runPlace(ctx context.Context, client *indira.Client, auth *indira.AuthContext, adapter *manthan.BrokerAdapter, userID string, bufferPct float64, dryRun bool, outFile string, includeSet map[string]bool) {
	fmt.Printf("Fetching holdings + order-book for %s...\n", userID)

	holdings, err := fetchHoldingsRaw(ctx, auth)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetchHoldingsRaw failed: %v\n", err)
		os.Exit(1)
	}
	rawOrders, err := fetchOrderBookRaw(ctx, auth)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: order-book fetch failed (will not de-dup): %v\n", err)
	}

	existingSL := buildExistingSLMapRaw(rawOrders)

	fmt.Printf("\nFound: holdings=%d  order_book=%d  existing_active_SL=%d\n\n",
		len(holdings), len(rawOrders), len(existingSL))

	results := []TestResult{}
	processed := map[string]bool{}

	for _, h := range holdings {
		listing := pickPrimaryListing(h.SymbolListings)
		if listing == nil {
			continue
		}
		if processed[listing.DispSym] {
			continue
		}
		if h.Qty <= 0 {
			continue
		}
		if len(includeSet) > 0 && !includeSet[strings.ToUpper(listing.DispSym)] {
			continue
		}
		if h.LTP < minLTPForTest {
			r := buildResult(listing.Symbol, listing.DispSym, listing.Exc,
				strconv.Itoa(listing.ExcTkn), "holding", h.Qty, h.LTP,
				parseTickSize(listing.TickSize), bufferPct)
			r.Skipped = true
			r.SkipReason = fmt.Sprintf("LTP ₹%.2f below ₹%.0f penny-stock cutoff", h.LTP, minLTPForTest)
			results = append(results, r)
			printResult(&r, dryRun)
			continue
		}
		r := buildResult(listing.Symbol, listing.DispSym, listing.Exc,
			strconv.Itoa(listing.ExcTkn), "holding", h.Qty, h.LTP,
			parseTickSize(listing.TickSize), bufferPct)
		applyExistingSLSkip(&r, existingSL)
		if !dryRun && !r.Skipped {
			placeAMOSL(ctx, adapter, auth, userID, &r)
		}
		results = append(results, r)
		printResult(&r, dryRun)
	}

	out := ResultsFile{
		UserID:      userID,
		SubmittedAt: time.Now(),
		BufferPct:   bufferPct,
		Tests:       results,
	}
	data, _ := json.MarshalIndent(&out, "", "  ")
	if err := os.WriteFile(outFile, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write %s: %v\n", outFile, err)
	} else {
		fmt.Printf("\nResults saved to %s\n", outFile)
	}
	if dryRun {
		fmt.Println("DRY RUN — no orders placed.")
		return
	}
	fmt.Printf("\nNext steps:\n")
	fmt.Printf("  1. Tomorrow ~9:16 IST run --verify to see which AMOs became live SL:\n")
	fmt.Printf("       --verify --out %s\n", outFile)
	fmt.Printf("  2. After verification, --cancel to clean up the test orders.\n")
}

// ───────────────────────────────────────── verify ─────────────────────────────────────────

func runVerify(ctx context.Context, client *indira.Client, auth *indira.AuthContext, inFile string) {
	saved, err := loadResults(inFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	book, err := fetchOrderBookRaw(ctx, auth)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetchOrderBookRaw failed: %v\n", err)
		os.Exit(1)
	}
	byID := map[string]*RawOrder{}
	for i := range book {
		byID[book[i].OrdId] = &book[i]
	}

	fmt.Printf("\nVerifying %d test orders submitted at %s\n",
		len(saved.Tests), saved.SubmittedAt.Format(time.RFC3339))
	fmt.Printf("\n%-12s %-20s %-18s %-10s %-10s %s\n",
		"SYMBOL", "BROKER_ID", "STATUS", "TRIGGER", "PRICE", "REJ_REASON")
	fmt.Println(strings.Repeat("─", 110))

	working, dead, missing, notPlaced := 0, 0, 0, 0
	for _, t := range saved.Tests {
		if t.BrokerOrderID == "" {
			notPlaced++
			fmt.Printf("%-12s %-20s %-18s\n", t.DispSym, "—", "NOT_PLACED")
			continue
		}
		bo, ok := byID[t.BrokerOrderID]
		if !ok {
			missing++
			fmt.Printf("%-12s %-20s %-18s\n", t.DispSym, t.BrokerOrderID, "NOT_IN_ORDER_BOOK")
			continue
		}
		st := strings.ToUpper(strings.TrimSpace(bo.Status))
		category := classifyStatus(st)
		switch category {
		case "working":
			working++
		case "dead":
			dead++
		default:
			missing++
		}
		fmt.Printf("%-12s %-20s %-18s %-10.2f %-10.2f %s\n",
			t.DispSym, bo.OrdId, bo.Status, bo.TriggerPrice, bo.Price, bo.RejReason)
	}
	fmt.Println(strings.Repeat("─", 110))
	fmt.Printf("Working (TRIGGER PENDING / OPEN / PENDING): %d\n", working)
	fmt.Printf("Dead    (CANCELLED / REJECTED / EXPIRED) : %d\n", dead)
	fmt.Printf("Missing or not placed                    : %d\n", missing+notPlaced)

	switch {
	case working > 0 && dead == 0:
		fmt.Println("\n✅ AMO+SL CONFIRMED — replayer can use 16:00 AMO submission")
	case working > 0 && dead > 0:
		fmt.Println("\n⚠️  PARTIAL SUCCESS — some converted, some didn't. Inspect rej_reason for the dead ones.")
	case working == 0 && dead > 0:
		fmt.Println("\n❌ AMO+SL NOT supported — replayer must hot-place at 9:15:30 instead")
	default:
		fmt.Println("\n❓ INCONCLUSIVE — orders missing from book, broker may have purged AMO queue")
	}
}

// ───────────────────────────────────────── cancel ─────────────────────────────────────────

func runCancel(ctx context.Context, client *indira.Client, auth *indira.AuthContext, inFile string) {
	saved, err := loadResults(inFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	cancelled, failed := 0, 0
	for _, t := range saved.Tests {
		if t.BrokerOrderID == "" {
			continue
		}
		req := &indira.CancelOrderRequest{
			Symbol: t.Symbol,
			Exc:    t.Exchange,
			OrdId:  t.BrokerOrderID,
		}
		if err := client.CancelOrder(ctx, auth, req); err != nil {
			failed++
			fmt.Printf("  ✗ %-12s %s — %v\n", t.DispSym, t.BrokerOrderID, err)
			continue
		}
		cancelled++
		fmt.Printf("  ✓ %-12s %s cancelled\n", t.DispSym, t.BrokerOrderID)
	}
	fmt.Printf("\nCancelled=%d  Failed=%d\n", cancelled, failed)
}

// ───────────────────────────────────────── helpers ─────────────────────────────────────────

func buildExistingSLMapRaw(book []RawOrder) map[string]string {
	out := map[string]string{}
	for _, ord := range book {
		if !strings.EqualFold(ord.OrdAction, "SELL") {
			continue
		}
		ot := strings.ToUpper(ord.OrdType)
		if !(strings.Contains(ot, "SL") || strings.Contains(ot, "STOP")) {
			continue
		}
		st := strings.ToUpper(strings.TrimSpace(ord.Status))
		if classifyStatus(st) != "working" {
			continue
		}
		key := strings.ToUpper(ord.Symbol.DispSym)
		if key != "" {
			out[key] = ord.OrdId
		}
	}
	return out
}

func applyExistingSLSkip(r *TestResult, existing map[string]string) {
	if id, ok := existing[strings.ToUpper(r.DispSym)]; ok {
		r.Skipped = true
		r.SkipReason = "active SL SELL already on broker: " + id
	}
}

func buildResult(symbol, disp, exc, excToken, src string, qty int, ltp, tickSize, bufferPct float64) TestResult {
	if tickSize <= 0 {
		tickSize = defaultTickSize
	}
	rawTrigger := ltp * (1 - bufferPct/100)
	trigger := roundToTick(rawTrigger, tickSize)
	limit := roundToTick(trigger-tickSize, tickSize)
	return TestResult{
		Symbol:       symbol,
		DispSym:      disp,
		Exchange:     exc,
		ExcToken:     excToken,
		Source:       src,
		Qty:          qty,
		LTP:          ltp,
		TriggerPrice: trigger,
		LimitPrice:   limit,
	}
}

// placeAMOSL routes through manthan.BrokerAdapter.PlaceAMOSLSell so the
// production-grade DPR clamp + 50bps overnight-safety buffer applies. This
// makes the test exercise the SAME code path the live replayer uses,
// eliminating the "test bypassed clamp" gap from the 2026-04-29 first run.
func placeAMOSL(ctx context.Context, adapter *manthan.BrokerAdapter, auth *indira.AuthContext, userID string, r *TestResult) {
	if r.LTP <= 0 || r.TriggerPrice <= 0 {
		r.Error = "missing LTP or trigger"
		return
	}

	// Resolve symbol via the adapter — pulls DPRLower / DPRUpper / TickSize
	// from the same external Redis the live service uses.
	info, err := adapter.ResolveSymbol(ctx, r.DispSym, "")
	if err != nil || info.IndiraSymbol == "" {
		// Fall back to the raw fields we already have. ResolveSymbol uses
		// ISIN → token; for a symbol-only test that path may miss. Build the
		// SymbolInfo from the holding row but DPR will be zero (no clamp).
		info = &manthan.SymbolInfo{
			Symbol:        r.DispSym,
			IndiraSymbol:  r.Symbol,
			ExchangeToken: r.ExcToken,
			Exchange:      r.Exchange,
			TickSize:      0.05,
		}
	} else {
		// Override with the test-CLI's already-resolved indira symbol/token
		// to avoid mismatch when ResolveSymbol's token map differs.
		info.IndiraSymbol = r.Symbol
		info.ExchangeToken = r.ExcToken
		info.Exchange = r.Exchange
	}

	brokerAuth := manthan.BrokerAuth{
		UserID:      userID,
		BearerToken: auth.BearerToken,
		AppID:       auth.AppId,
		Source:      auth.Source,
	}

	brokerOrderID, err := adapter.PlaceAMOSLSell(ctx, brokerAuth, info, r.Qty, r.TriggerPrice, r.LimitPrice)
	if err != nil {
		r.Error = err.Error()
		return
	}
	r.BrokerOrderID = brokerOrderID
	r.OrdStatus = "Requested"
}

func loadResults(path string) (*ResultsFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var saved ResultsFile
	if err := json.Unmarshal(data, &saved); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &saved, nil
}

func classifyStatus(st string) string {
	switch {
	case strings.Contains(st, "TRIGGER"),
		st == "OPEN", st == "PENDING", st == "REQUESTED", st == "MODIFIED",
		st == "PLACED", st == "AMO":
		return "working"
	case st == "CANCELLED", st == "CANCELED", st == "REJECTED", st == "EXPIRED",
		st == "EXECUTED", st == "TRADED", st == "COMPLETE", st == "FILLED":
		return "dead"
	}
	return "unknown"
}

func roundToTick(price, tick float64) float64 {
	if tick <= 0 {
		return price
	}
	return math.Round(price/tick) * tick
}

func parseTickSize(s string) float64 {
	if s == "" {
		return defaultTickSize
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return defaultTickSize
	}
	return v
}

// ─────────────────── raw holdings fetch (broker shape doesn't match pkg/indira.Holding) ───────────────────

type RawHoldingListing struct {
	Symbol   string `json:"symbol"`
	DispSym  string `json:"dispSym"`
	Exc      string `json:"exc"`
	ExcTkn   int    `json:"excTkn"`
	TickSize string `json:"tickSize"`
	ISIN     string `json:"isin"`
}

type RawHolding struct {
	SymbolListings []RawHoldingListing `json:"symbol"`
	LTP            float64             `json:"ltp"`
	Qty            int                 `json:"qty"`
	HoldingQty     int                 `json:"holdingQty"`
	AvgPrice       float64             `json:"avgPrice"`
	PrdType        string              `json:"prdType"`
}

func fetchHoldingsRaw(ctx context.Context, auth *indira.AuthContext) ([]RawHolding, error) {
	url := "https://livemiddleware.indiratrade.com/portfolio-services/api/portfolio/v2/holdings"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+auth.BearerToken)
	req.Header.Set("appId", auth.AppId)
	req.Header.Set("userId", auth.UserId)
	req.Header.Set("source", auth.Source)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("sso", "True") // case-sensitive — pkg/indira/client.go calls this out

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(body))
	}
	var wrap struct {
		Data struct {
			Holdings []RawHolding `json:"holdings"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &wrap); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	return wrap.Data.Holdings, nil
}

// ─────────────────── raw order-book fetch (broker shape) ───────────────────

type RawOrderSymbol struct {
	Symbol  string `json:"symbol"`
	DispSym string `json:"dispSym"`
	Exc     string `json:"exc"`
	ExcTkn  int    `json:"excTkn"`
}

type RawOrder struct {
	OrdId        string         `json:"ordId"`
	Symbol       RawOrderSymbol `json:"symbol"`
	Status       string         `json:"status"`
	OrdAction    string         `json:"ordAction"`
	OrdType      string         `json:"ordType"`
	OrdValidity  string         `json:"ordValidity"`
	PrdType      string         `json:"prdType"`
	Price        float64        `json:"price"`
	TriggerPrice float64        `json:"triggerPrice"`
	RejReason    string         `json:"rejReason"`
	Qty          int            `json:"qty"`
	TradedQty    int            `json:"tradedQty"`
	AMO          bool           `json:"amo"`
}

func fetchOrderBookRaw(ctx context.Context, auth *indira.AuthContext) ([]RawOrder, error) {
	url := "https://livemiddleware.indiratrade.com/portfolio-services/api/order/v1/order-book"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+auth.BearerToken)
	req.Header.Set("appId", auth.AppId)
	req.Header.Set("userId", auth.UserId)
	req.Header.Set("source", auth.Source)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("sso", "True")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(body))
	}
	var wrap struct {
		Data struct {
			Orders []RawOrder `json:"orders"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &wrap); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	return wrap.Data.Orders, nil
}

// pickPrimaryListing returns the NSE listing if present, else the first listing.
// Indira returns each holding with both NSE+BSE listings; we use NSE for orders.
func pickPrimaryListing(listings []RawHoldingListing) *RawHoldingListing {
	for i := range listings {
		if strings.EqualFold(listings[i].Exc, "NSE") {
			return &listings[i]
		}
	}
	if len(listings) > 0 {
		return &listings[0]
	}
	return nil
}

func printResult(r *TestResult, dryRun bool) {
	prefix := "✓"
	body := fmt.Sprintf("%-12s qty=%-3d ltp=%-9.2f trigger=%-9.2f limit=%-9.2f",
		r.DispSym, r.Qty, r.LTP, r.TriggerPrice, r.LimitPrice)
	switch {
	case r.Skipped:
		prefix = "⊘"
		body += " SKIP: " + r.SkipReason
	case dryRun:
		prefix = "·"
		body += " (dry-run)"
	case r.Error != "":
		prefix = "✗"
		body += " ERROR: " + r.Error
	case strings.EqualFold(r.OrdStatus, "Rejected"):
		prefix = "✗"
		body += fmt.Sprintf(" REJECTED: %s", r.RejReason)
	case r.BrokerOrderID != "":
		body += fmt.Sprintf(" → %s [%s]", r.BrokerOrderID, r.OrdStatus)
	}
	fmt.Printf("  %s %s\n", prefix, body)
}
