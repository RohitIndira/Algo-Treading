// broadcastprobe — read-only check of Indira's dealer broadcast order feed:
// does ONE session (e.g. S4450's) receive order status frames for ALL clients?
//
// It never places, modifies or cancels anything. The session token is passed
// by the operator and never stored.
//
//	INDIRA_SESSION='<S4450 SSO bearer token>' go run ./services/orderstatus/cmd/broadcastprobe -user S4450 -minutes 3
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	tokenURL = "https://livemiddleware.indiratrade.com/order-notify/ws/createWsToken"
	wsURL    = "wss://livemiddleware.indiratrade.com/order-notify/websocket/broadcast"
)

func main() {
	user := flag.String("user", "", "client id whose session anchors the feed (e.g. S4450)")
	minutes := flag.Int("minutes", 3, "how long to listen")
	raw := flag.Bool("raw", false, "print every frame as raw JSON")
	flag.Parse()
	session := strings.TrimSpace(os.Getenv("INDIRA_SESSION"))
	if *user == "" || session == "" {
		fmt.Fprintln(os.Stderr, "need -user and INDIRA_SESSION (bearer token of that user's live session)")
		os.Exit(2)
	}

	// Step 1 — websocket token from the session.
	req, _ := http.NewRequest(http.MethodGet, tokenURL, nil)
	req.Header.Set("Authorization", "Bearer "+session)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "token request:", err)
		os.Exit(1)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var tok struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Result  []struct {
			OrderToken string `json:"orderToken"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &tok); err != nil || len(tok.Result) == 0 || tok.Result[0].OrderToken == "" {
		fmt.Fprintf(os.Stderr, "token response HTTP %d: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	fmt.Printf("ws token ok (status=%s)\n", tok.Status)

	// Step 2 — connect, handshake, heartbeat.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	conn, hresp, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		code := 0
		if hresp != nil {
			code = hresp.StatusCode
		}
		fmt.Fprintf(os.Stderr, "dial: %v (http %d)\n", err, code)
		os.Exit(1)
	}
	defer conn.Close()
	hello, _ := json.Marshal(map[string]string{"userId": *user, "orderToken": tok.Result[0].OrderToken})
	if err := conn.WriteMessage(websocket.TextMessage, hello); err != nil {
		fmt.Fprintln(os.Stderr, "handshake write:", err)
		os.Exit(1)
	}
	fmt.Printf("connected to %s as %s; listening %d min (Ctrl-C to stop early)\n", wsURL, *user, *minutes)

	go func() {
		t := time.NewTicker(50 * time.Second)
		defer t.Stop()
		hb, _ := json.Marshal(map[string]string{"userId": *user, "heartbeat": "h"})
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = conn.WriteMessage(websocket.TextMessage, hb)
			}
		}
	}()

	deadline := time.Now().Add(time.Duration(*minutes) * time.Minute)
	perUCC := map[string]int{}
	frames, other := 0, 0
	for time.Now().Before(deadline) && ctx.Err() == nil {
		_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
				continue // quiet feed — keep waiting
			}
			fmt.Fprintln(os.Stderr, "read:", err)
			break
		}
		var f map[string]any
		if json.Unmarshal(msg, &f) != nil {
			other++
			fmt.Printf("%s non-json: %.120s\n", time.Now().Format("15:04:05"), string(msg))
			continue
		}
		ucc, _ := f["UCC"].(string)
		if ucc == "" {
			other++
			fmt.Printf("%s control: %.200s\n", time.Now().Format("15:04:05"), string(msg))
			continue
		}
		frames++
		perUCC[ucc]++
		if *raw {
			fmt.Println(string(msg))
			continue
		}
		fmt.Printf("%s UCC=%-10s %-14s %-4v %-22v qty=%v traded=%v@%v id=%v amo=%v\n",
			time.Now().Format("15:04:05"), ucc, f["Symbol"], f["Buy_Sell"], f["OrderStatus"],
			f["OrderOriginalQty"], f["TradedQTY"], f["TradedPrice"], f["UniqueCode"], f["AMOOrderID"])
	}
	fmt.Printf("\n=== summary: %d order frames, %d control/other frames, %d distinct clients ===\n", frames, other, len(perUCC))
	keys := make([]string, 0, len(perUCC))
	for k := range perUCC {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-12s %d\n", k, perUCC[k])
	}
}
