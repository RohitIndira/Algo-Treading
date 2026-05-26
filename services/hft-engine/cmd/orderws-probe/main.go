// orderws-probe — standalone diagnostic for Indira's order-notify WebSocket.
//
// Purpose:
//   - Connect to wss://livemiddleware.indiratrade.com/order-notify/websocket
//     with whatever creds you give it.
//   - Print EVERY raw frame the broker sends — no struct, no field mapping,
//     so nothing is silently dropped by a naming mismatch.
//   - For trade/order frames, also print a highlighted summary line that
//     pulls price + qty fields under every name the broker is known to use
//     (TradedPrice vs OrderPrice, TradedQTY vs TradeQty vs QuantityTradedToday).
//
// This is the minimal reproducer for "does the order WS actually carry a
// real fill price?" — the engine currently reads TradedPrice, which the
// broker frequently sends as "0.00". This probe shows whether OrderPrice
// (or any other field) holds the real number for our own LIMIT orders.
//
// It does NOT touch Postgres, Redis, the strategy engine, or place orders.
//
// Examples:
//
//	# Direct order-token (skip the REST token exchange).
//	go run ./services/hft-engine/cmd/orderws-probe/ \
//	    --user-id ISPL19122 --order-token dc534... --duration 5m
//
//	# Fetch a fresh order-token from a JWT bearer.
//	go run ./services/hft-engine/cmd/orderws-probe/ \
//	    --user-id ISPL19122 --bearer eyJhbGci... --duration 5m
//
// Run it locally during market hours, then place / let the engine place an
// order on that account. Watch the TRD frames to learn which field is real.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsEndpoint = "wss://livemiddleware.indiratrade.com/order-notify/websocket"
	tokenAPI   = "https://livemiddleware.indiratrade.com/order-notify/ws/createWsToken"
)

func main() {
	var (
		userID    = flag.String("user-id", "", "Indira user id, e.g. ISPL19122 (required)")
		appID     = flag.String("app-id", "", "Indira appId — required by the token API when using --bearer")
		source    = flag.String("source", "WEB", "Indira source header (WEB/AND/IOS)")
		orderTok  = flag.String("order-token", "", "WS order token — if empty, fetched via --bearer")
		bearer    = flag.String("bearer", "", "JWT bearer token, used to fetch an order token when --order-token is empty")
		duration  = flag.Duration("duration", 5*time.Minute, "How long to listen before exiting")
		allFrames = flag.Bool("all", false, "Print heartbeats and non-trade frames too (default: trade frames only)")
	)
	flag.Parse()

	if *userID == "" {
		log.Fatal("--user-id is required")
	}
	if *orderTok == "" && *bearer == "" {
		log.Fatal("provide either --order-token or --bearer")
	}

	// ── resolve the order token ──────────────────────────────────────────
	token := *orderTok
	if token == "" {
		t, err := fetchOrderToken(*userID, *appID, *source, *bearer)
		if err != nil {
			log.Fatalf("fetch order token: %v", err)
		}
		token = t
		log.Printf("fetched order token: %s", mask(token))
	}

	log.Printf("──────────────────────────────────────────────────────────")
	log.Printf(" orderws-probe")
	log.Printf(" target:    %s", wsEndpoint)
	log.Printf(" user_id:   %s", *userID)
	log.Printf(" token:     %s", mask(token))
	log.Printf(" duration:  %s", *duration)
	log.Printf("──────────────────────────────────────────────────────────")

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("interrupt — shutting down")
		cancel()
	}()

	// ── dial ─────────────────────────────────────────────────────────────
	conn, _, err := websocket.DefaultDialer.Dial(wsEndpoint, nil)
	if err != nil {
		log.Fatalf("websocket dial failed: %v", err)
	}
	defer conn.Close()

	authMsg, _ := json.Marshal(map[string]string{"userId": *userID, "orderToken": token})
	if err := conn.WriteMessage(websocket.TextMessage, authMsg); err != nil {
		log.Fatalf("send auth: %v", err)
	}
	log.Printf("auth frame sent — listening...")

	// ── heartbeat writer ─────────────────────────────────────────────────
	hb, _ := json.Marshal(map[string]string{"userId": *userID, "heartbeat": "h"})
	go func() {
		ticker := time.NewTicker(45 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, hb); err != nil {
					return
				}
			}
		}
	}()

	// ── read loop ────────────────────────────────────────────────────────
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	var (
		total   int
		trades  int
		started = time.Now()
	)
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("read error: %v", err)
			break
		}
		total++
		printFrame(raw, *allFrames, &trades)
	}

	log.Printf("──────────────────────────────────────────────────────────")
	log.Printf(" SUMMARY  ran %s", time.Since(started).Round(time.Second))
	log.Printf(" frames received: %d  (of which %d were order/trade frames)", total, trades)
	log.Printf("──────────────────────────────────────────────────────────")
	if trades == 0 {
		log.Println()
		log.Println("⚠  ZERO order/trade frames — auth ok but no order activity.")
		log.Println("   Place an order on this account while the probe runs,")
		log.Println("   or run it during a live HFT strategy session.")
		os.Exit(2)
	}
}

// printFrame renders one raw WS frame. Status/control frames and heartbeats
// are shown only with --all. Order/trade frames always print: the raw JSON
// plus a parsed summary that surfaces price + qty under every known field
// name so we can see which one the broker actually populates.
func printFrame(raw []byte, all bool, trades *int) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		if all {
			fmt.Printf("[%s] (non-JSON) %s\n", now(), string(raw))
		}
		return
	}

	// Heartbeat echo / status ack frames.
	if _, hb := m["heartbeat"]; hb {
		if all {
			fmt.Printf("[%s] heartbeat\n", now())
		}
		return
	}
	if st, ok := m["status"]; ok && len(m) <= 2 {
		fmt.Printf("[%s] status: %v\n", now(), st)
		return
	}

	// Is this an order/trade frame?
	_, hasStatus := m["OrderStatus"]
	_, hasMsgType := m["MessageType"]
	if !hasStatus && !hasMsgType {
		if all {
			fmt.Printf("[%s] (other) %s\n", now(), string(raw))
		}
		return
	}

	*trades++
	fmt.Printf("\n[%s] ── ORDER/TRADE FRAME ──────────────────────────────\n", now())
	fmt.Printf("  MessageType=%v  OrderStatus=%v  OrderType=%v  Buy_Sell=%v\n",
		m["MessageType"], m["OrderStatus"], m["OrderType"], m["Buy_Sell"])
	fmt.Printf("  OrderNumber=%v  UniqueCode=%v  Symbol=%v\n",
		m["OrderNumber"], m["UniqueCode"], m["Symbol"])
	fmt.Printf("  PRICE  → TradedPrice=%-10v OrderPrice=%-10v TriggerPrice=%v\n",
		field(m, "TradedPrice"), field(m, "OrderPrice"), field(m, "TriggerPrice"))
	fmt.Printf("  QTY    → TradedQTY=%-7v TradeQty=%-7v QuantityTradedToday=%-7v PendingQty=%v OrigQty=%v\n",
		field(m, "TradedQTY"), field(m, "TradeQty"), field(m, "QuantityTradedToday"),
		field(m, "PendingQty"), field(m, "OrderOriginalQty"))
	if r := field(m, "Reason"); r != "" && r != "<nil>" {
		fmt.Printf("  Reason=%v\n", r)
	}
	fmt.Printf("  raw: %s\n", string(raw))
}

func field(m map[string]any, k string) string {
	v, ok := m[k]
	if !ok {
		return "·"
	}
	return fmt.Sprintf("%v", v)
}

// fetchOrderToken exchanges a JWT bearer for a fresh WS order token via the
// REST API — same call pkg/indira's WSClient.GetWebSocketToken makes.
func fetchOrderToken(userID, appID, source, bearer string) (string, error) {
	req, err := http.NewRequest("GET", tokenAPI, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("userId", userID)
	if appID != "" {
		req.Header.Set("appId", appID)
	}
	if source != "" {
		req.Header.Set("source", source)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("sso", "True")
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var tr struct {
		Status string `json:"status"`
		Result []struct {
			OrderToken string `json:"orderToken"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", fmt.Errorf("unmarshal token response: %w (body=%s)", err, string(body))
	}
	if len(tr.Result) == 0 || tr.Result[0].OrderToken == "" {
		return "", fmt.Errorf("no order token in response: %s", string(body))
	}
	return tr.Result[0].OrderToken, nil
}

func now() string { return time.Now().Format("15:04:05.000") }

func mask(s string) string {
	if len(s) <= 8 {
		return "****"
	}
	return s[:4] + "…" + s[len(s)-4:]
}
