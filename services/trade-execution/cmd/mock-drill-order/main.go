// mock-drill-order — one-shot CLI for exchange mock-drill sessions.
//
// Runs the Manthan order sequence against the REAL broker for one stock on a
// chosen exchange (BSE or NSE) and captures every broker response + the
// resulting order-book rows to a JSON file:
//
//   1. BUY  Limit  DELIVERY DAY   @ --price            (entry)
//   2. SELL SL     DELIVERY GTC   trigger = price*(1-sl%)  limit = trigger-0.5% (protective stop, Manthan 20% rule)
//   3. order-book snapshot (raw)
//
// USAGE:
//   JWT='eyJ...' APP_ID='...' USER_ID='ND03920' \
//     go run ./services/trade-execution/cmd/mock-drill-order/ \
//       --exc BSE --symbol STK_RELIANCE_EQ_BSE_500325 --token 500325 --price 1400 --qty 1
//
//   --dry-run prints the payloads without submitting.
//   --sl-pct  defaults to 20 (hard Manthan rule).
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
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

type capture struct {
	Step     string      `json:"step"`
	At       string      `json:"at"`
	Request  interface{} `json:"request,omitempty"`
	Response interface{} `json:"response,omitempty"`
	Error    string      `json:"error,omitempty"`
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func main() {
	exc := flag.String("exc", "BSE", "exchange: BSE or NSE")
	symbol := flag.String("symbol", "", "Indira symbol, e.g. STK_RELIANCE_EQ_BSE_500325")
	token := flag.String("token", "", "exchange token, e.g. 500325")
	price := flag.Float64("price", 0, "entry limit price")
	qty := flag.Int("qty", 1, "quantity")
	slPct := flag.Float64("sl-pct", 20, "stop-loss percent below entry")
	dryRun := flag.Bool("dry-run", false, "print payloads, do not submit")
	skipSL := flag.Bool("skip-sl", false, "place entry only")
	snapshot := flag.Bool("snapshot", false, "only fetch + print the order book (no orders placed)")
	cases := flag.Bool("cases", false, "run the auditor-checklist probe set on --exc/--symbol/--token (LTP from EXT_REDIS_ADDR/EXT_REDIS_PASSWORD, or --price)")
	stopStrategy := flag.String("stop-strategy", "", "strategy_id: call our api-gateway DELETE /api/v1/strategies/{id} with position_handling=SQUARE_OFF_AT_MARKET (gateway → user-config → trade-execution force-exit)")
	gatewayURL := flag.String("gateway", "http://127.0.0.1:8080", "api-gateway base URL for --stop-strategy")
	positionHandling := flag.String("position-handling", "SQUARE_OFF_AT_MARKET", "SQUARE_OFF_AT_MARKET | KEEP_POSITIONS_OPEN (for --stop-strategy)")
	baseURL := flag.String("base-url", "https://livemiddleware.indiratrade.com", "broker base URL")
	out := flag.String("out", "", "output JSON (default ./mock-drill-USER-EXC-SYMBOL-ts.json)")
	ucAddr := flag.String("user-config", "", "user-config gRPC addr (e.g. 127.0.0.1:50051) — fetch decrypted creds for USER_ID instead of JWT/APP_ID env")
	flag.Parse()

	jwt := strings.TrimSpace(os.Getenv("JWT"))
	appID := strings.TrimSpace(os.Getenv("APP_ID"))
	userID := strings.TrimSpace(os.Getenv("USER_ID"))
	source := os.Getenv("SOURCE")
	if *ucAddr != "" && userID != "" {
		conn, err := grpc.NewClient(*ucAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, "dial user-config:", err)
			os.Exit(2)
		}
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := pb.NewUserConfigServiceClient(conn).GetUserCredentials(cctx, &pb.GetUserCredentialsRequest{UserId: userID})
		ccancel()
		conn.Close()
		if err != nil || resp == nil || !resp.Success || resp.IndiraAuth == nil {
			fmt.Fprintf(os.Stderr, "user-config GetUserCredentials failed: err=%v resp=%+v\n", err, resp)
			os.Exit(2)
		}
		jwt, appID, source = resp.IndiraAuth.BearerToken, resp.IndiraAuth.AppId, resp.IndiraAuth.Source
		fmt.Printf("creds from user-config: user=%s app=%s source=%s jwt_len=%d\n", resp.IndiraAuth.UserId, appID, source, len(jwt))
	}
	if *snapshot && jwt != "" && appID != "" && userID != "" {
		client := indira.NewClient(indira.Config{BaseURL: *baseURL, Timeout: 20 * time.Second})
		auth := &indira.AuthContext{UserId: userID, AppId: appID, ClientId: userID, Source: source, BearerToken: jwt, SSO: true}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		raw, err := client.GetOrderBookRaw(ctx, auth)
		if err != nil {
			fmt.Fprintln(os.Stderr, "order-book:", err)
			os.Exit(1)
		}
		fmt.Println(string(raw))
		return
	}
	if *stopStrategy != "" && *gatewayURL == "grpc" && *ucAddr != "" && jwt != "" {
		// Direct user-config gRPC path (same hop the gateway makes after auth):
		// user-config → trade-execution force-exit → lifecycle DELETED.
		conn, err := grpc.NewClient(*ucAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, "dial user-config:", err)
			os.Exit(1)
		}
		defer conn.Close()
		ph := pb.PositionHandling_SQUARE_OFF_AT_MARKET
		if *positionHandling == "KEEP_POSITIONS_OPEN" {
			ph = pb.PositionHandling_KEEP_POSITIONS_OPEN
		}
		req := &pb.DeleteStrategyRequest{
			StrategyId:       *stopStrategy,
			UserId:           userID,
			PositionHandling: ph,
			IndiraAuth:       &common.IndiraAuthContext{UserId: userID, AppId: appID, ClientId: userID, Source: source, BearerToken: jwt},
		}
		fmt.Printf("[%s] user-config gRPC DeleteStrategy strategy_id=%s user_id=%s position_handling=%s (indira_auth: user=%s app=%s source=%s jwt len %d)\n",
			time.Now().Format(time.RFC3339Nano), *stopStrategy, userID, ph.String(), userID, appID, source, len(jwt))
		cctx, ccancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer ccancel()
		resp, err := pb.NewUserConfigServiceClient(conn).DeleteStrategy(cctx, req)
		fmt.Printf("[%s] response: success=%v positions_exited=%d message=%q error=%v rpc_err=%v\n",
			time.Now().Format(time.RFC3339Nano), resp.GetSuccess(), resp.GetPositionsExited(), resp.GetMessage(), resp.GetError(), err)
		return
	}
	if *stopStrategy != "" && jwt != "" && appID != "" && userID != "" {
		body := fmt.Sprintf(`{"user_id":%q,"position_handling":%q}`, userID, *positionHandling)
		req, err := http.NewRequest(http.MethodDelete, strings.TrimRight(*gatewayURL, "/")+"/api/v1/strategies/"+*stopStrategy, strings.NewReader(body))
		if err != nil {
			fmt.Fprintln(os.Stderr, "build request:", err)
			os.Exit(1)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+jwt)
		req.Header.Set("userId", userID)
		req.Header.Set("appId", appID)
		req.Header.Set("source", source)
		fmt.Printf("[%s] DELETE %s body=%s (headers: userId=%s appId=%s source=%s Authorization=Bearer <jwt len %d>)\n",
			time.Now().Format(time.RFC3339Nano), req.URL.String(), body, userID, appID, source, len(jwt))
		resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, "gateway call failed:", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		rb, _ := io.ReadAll(resp.Body)
		fmt.Printf("[%s] status=%d body=%s\n", time.Now().Format(time.RFC3339Nano), resp.StatusCode, string(rb))
		return
	}
	if *cases && jwt != "" && appID != "" && userID != "" && *symbol != "" && *token != "" {
		runCases(*baseURL, userID, appID, source, jwt, *exc, *symbol, *token, *price, *slPct, *out)
		return
	}
	if jwt == "" || appID == "" || userID == "" || *symbol == "" || *token == "" || *price <= 0 {
		fmt.Fprintln(os.Stderr, "need JWT, APP_ID, USER_ID env (or --user-config + USER_ID) + --symbol --token --price")
		os.Exit(2)
	}
	if source == "" {
		source = "WEB"
	}

	client := indira.NewClient(indira.Config{BaseURL: *baseURL, Timeout: 20 * time.Second})
	auth := &indira.AuthContext{UserId: userID, AppId: appID, ClientId: userID, Source: source, BearerToken: jwt, SSO: true}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var caps []capture
	now := func() string { return time.Now().Format(time.RFC3339Nano) }

	entry := &indira.PlaceOrderRequest{
		Symbol: *symbol, ExcToken: *token, Exc: *exc,
		OrdAction: "BUY", OrdValidity: "DAY", OrdType: "Limit", PrdType: "DELIVERY",
		LimitPrice: indira.Price2DP(round2(*price)), Qty: *qty, LotSize: 1, Instrument: "STK",
	}
	trig := round2(*price * (1 - *slPct/100))
	lim := round2(trig * 0.995)
	sl := &indira.PlaceOrderRequest{
		Symbol: *symbol, ExcToken: *token, Exc: *exc,
		OrdAction: "SELL", OrdValidity: "GTC", OrdType: "SL", PrdType: "DELIVERY",
		LimitPrice: indira.Price2DP(lim), TriggerPrice: indira.Price2DP(trig),
		Qty: *qty, LotSize: 1, Instrument: "STK",
	}

	if *dryRun {
		b, _ := json.MarshalIndent(map[string]interface{}{"entry": entry, "sl": sl}, "", "  ")
		fmt.Println(string(b))
		return
	}

	// 1. entry
	fmt.Printf("[%s] placing ENTRY %s BUY %d @ %.2f on %s\n", now(), *symbol, *qty, *price, *exc)
	resp, err := client.PlaceOrder(ctx, auth, entry)
	c := capture{Step: "entry_place", At: now(), Request: entry, Response: resp}
	if err != nil {
		c.Error = err.Error()
	}
	caps = append(caps, c)
	fmt.Printf("  -> resp=%+v err=%v\n", resp, err)

	// 2. SL
	if !*skipSL {
		time.Sleep(2 * time.Second)
		fmt.Printf("[%s] placing SL SELL %d trig=%.2f lim=%.2f GTC\n", now(), *qty, trig, lim)
		resp2, err2 := client.PlaceOrder(ctx, auth, sl)
		c2 := capture{Step: "sl_place", At: now(), Request: sl, Response: resp2}
		if err2 != nil {
			c2.Error = err2.Error()
		}
		caps = append(caps, c2)
		fmt.Printf("  -> resp=%+v err=%v\n", resp2, err2)
	}

	// 3. order book snapshot
	time.Sleep(3 * time.Second)
	raw, err3 := client.GetOrderBookRaw(ctx, auth)
	c3 := capture{Step: "order_book", At: now()}
	if err3 != nil {
		c3.Error = err3.Error()
	} else {
		var v interface{}
		if json.Unmarshal(raw, &v) == nil {
			c3.Response = v
		} else {
			c3.Response = string(raw)
		}
	}
	caps = append(caps, c3)

	if *out == "" {
		*out = fmt.Sprintf("mock-drill-%s-%s-%s-%s.json", userID, *exc, *token, time.Now().Format("20060102-150405"))
	}
	b, _ := json.MarshalIndent(caps, "", "  ")
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
	}
	fmt.Println("captured ->", *out)
	// print order book rows for our token
	if s, ok := c3.Response.(string); ok {
		fmt.Println(s)
	} else if c3.Response != nil {
		bb, _ := json.Marshal(c3.Response)
		fmt.Println(truncate(string(bb), 3000))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ---------------------------------------------------------------------------
// Auditor-checklist probe set (Mock Market Session 2026 v1.0, individual level)
//
//	A1 resting LIMIT entry inside band          → Pending, cancelled in A8
//	A2 price-band breach HIGH (+25 % over LTP)  → must be rejected (Price Check)
//	A3 price-band breach LOW  (−30 % under LTP) → must be rejected (Price Check)
//	A4 per-order quantity-limit breach          → must be rejected (Quantity Check)
//	A5 per-order value-limit breach (≈ ₹12 cr)  → must be rejected (Order Value Check)
//	A6 MARKET entry (adapter's PlaceMarketBuy)  → fill + trade confirmation
//	A7 SL-L GTC at −20 % on the A6 position     → Manthan hard 20 % stop
//	A8 cancel the resting A1 order               → cancel request/response
//	A9/A10 order-book + trade-book confirmation
//
// Every request/response/timestamp is appended to the JSON capture.
// ---------------------------------------------------------------------------

func tick(v float64, t float64) float64 { return math.Round(v/t) * t }

func runCases(baseURL, userID, appID, source, jwt, exc, symbol, token string, price, slPct float64, out string) {
	client := indira.NewClient(indira.Config{BaseURL: baseURL, Timeout: 20 * time.Second})
	auth := &indira.AuthContext{UserId: userID, AppId: appID, ClientId: userID, Source: source, BearerToken: jwt, SSO: true}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	now := func() string { return time.Now().Format(time.RFC3339Nano) }
	var caps []capture
	rec := func(c capture) {
		caps = append(caps, c)
		b, _ := json.Marshal(c)
		fmt.Println(truncate(string(b), 900))
	}

	// 0. market snapshot for LTP / bands — same Redis key the broker adapter uses
	ltp := price
	if addr := strings.TrimSpace(os.Getenv("EXT_REDIS_ADDR")); addr != "" {
		r := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("EXT_REDIS_PASSWORD")})
		key := "market:" + strings.ToLower(exc) + ":" + token
		raw, err := r.Get(ctx, key).Result()
		c := capture{Step: "A0_market_snapshot", At: now(), Request: key}
		if err != nil {
			c.Error = err.Error()
		} else {
			var mkt map[string]interface{}
			_ = json.Unmarshal([]byte(raw), &mkt)
			c.Response = mkt
			if v, ok := mkt["ltp"].(float64); ok && v > 0 && ltp <= 0 {
				ltp = v
			}
		}
		rec(c)
	}
	if ltp <= 0 {
		fmt.Fprintln(os.Stderr, "no LTP: set EXT_REDIS_ADDR/EXT_REDIS_PASSWORD or pass --price")
		os.Exit(2)
	}
	fmt.Printf("LTP=%.2f on %s %s\n", ltp, exc, symbol)

	mk := func(action, validity, otype string, limit, trig float64, qty int) *indira.PlaceOrderRequest {
		return &indira.PlaceOrderRequest{Symbol: symbol, ExcToken: token, Exc: exc, OrdAction: action, OrdValidity: validity,
			OrdType: otype, PrdType: "DELIVERY", LimitPrice: indira.Price2DP(limit), TriggerPrice: indira.Price2DP(trig),
			Qty: qty, LotSize: 1, Instrument: "STK"}
	}
	place := func(step string, req *indira.PlaceOrderRequest) string {
		time.Sleep(1200 * time.Millisecond) // stay far below the broker's 10-orders/sec gate
		resp, err := client.PlaceOrder(ctx, auth, req)
		c := capture{Step: step, At: now(), Request: req, Response: resp}
		if err != nil {
			c.Error = err.Error()
		}
		rec(c)
		if resp != nil {
			if resp.OrdId != "" {
				return resp.OrdId
			}
			return resp.OrderId
		}
		return ""
	}

	ids := map[string]string{}
	ids["A1"] = place("A1_entry_limit_resting", mk("BUY", "DAY", "Limit", tick(ltp*0.97, 0.05), 0, 1))
	ids["A2"] = place("A2_price_band_breach_high", mk("BUY", "DAY", "Limit", tick(ltp*1.25, 0.05), 0, 1))
	ids["A3"] = place("A3_price_band_breach_low", mk("BUY", "DAY", "Limit", tick(ltp*0.70, 0.05), 0, 1))
	ids["A4"] = place("A4_qty_limit_breach", mk("BUY", "DAY", "Limit", tick(ltp, 0.05), 0, 5000000))
	ids["A5"] = place("A5_order_value_breach", mk("BUY", "DAY", "Limit", tick(ltp, 0.05), 0, int(120000000/ltp)))
	ids["A6"] = place("A6_entry_market", mk("BUY", "DAY", "Market", 0, 0, 1))
	trig := tick(ltp*(1-slPct/100), 0.05)
	ids["A7"] = place("A7_sl_20pct_gtc", mk("SELL", "GTC", "SL", tick(trig*0.995, 0.05), trig, 1))

	if id := ids["A1"]; id != "" {
		time.Sleep(1200 * time.Millisecond)
		req := &indira.CancelOrderRequest{Symbol: symbol, Exc: exc, OrdId: id}
		c := capture{Step: "A8_cancel_resting", At: now(), Request: req}
		if err := client.CancelOrder(ctx, auth, req); err != nil {
			c.Error = err.Error()
		} else {
			c.Response = "cancel accepted"
		}
		rec(c)
	}

	time.Sleep(4 * time.Second)
	ordIDs := []string{}
	for _, v := range ids {
		if v != "" {
			ordIDs = append(ordIDs, v)
		}
	}
	raw, err := client.GetOrderBookRaw(ctx, auth)
	c := capture{Step: "A9_order_book", At: now()}
	if err != nil {
		c.Error = err.Error()
	} else {
		var ob struct {
			Data struct {
				Orders []map[string]interface{} `json:"orders"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &ob) == nil {
			mine := []map[string]interface{}{}
			for _, o := range ob.Data.Orders {
				id, _ := o["ordId"].(string)
				for _, want := range ordIDs {
					if id == want {
						delete(o, "symbol")
						mine = append(mine, o)
					}
				}
			}
			c.Response = mine
		} else {
			c.Response = string(raw)
		}
	}
	rec(c)
	trades, err := client.GetTradeBook(ctx, auth, ordIDs...)
	c = capture{Step: "A10_trade_book", At: now(), Request: ordIDs, Response: trades}
	if err != nil {
		c.Error = err.Error()
	}
	rec(c)

	if out == "" {
		out = fmt.Sprintf("mock-drill-cases-%s-%s-%s-%s.json", userID, exc, token, time.Now().Format("20060102-150405"))
	}
	b, _ := json.MarshalIndent(caps, "", "  ")
	if err := os.WriteFile(out, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
	}
	fmt.Println("captured ->", out)
}
