// Command wss-capture is a verification tool: it connects to the Indira
// order-status WebSocket and prints every raw frame with a high-resolution
// receive timestamp, so a live order's fill frame can be captured and its
// latency/fields analysed. Not part of any service.
//
// Env: WS_USER (e.g. S4450), WS_TOKEN (orderToken from createWsToken),
//      WS_SECONDS (capture window, default 45).
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

const endpoint = "wss://livemiddleware.indiratrade.com/order-notify/websocket"

func ts() string { return time.Now().Format("15:04:05.000000") }

func main() {
	userID := os.Getenv("WS_USER")
	token := os.Getenv("WS_TOKEN")
	if userID == "" || token == "" {
		fmt.Println("WS_USER and WS_TOKEN are required")
		os.Exit(1)
	}
	secs := 45
	if s := os.Getenv("WS_SECONDS"); s != "" {
		fmt.Sscanf(s, "%d", &secs)
	}

	c, resp, err := websocket.DefaultDialer.Dial(endpoint, nil)
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		fmt.Printf("[%s] DIAL FAILED (http=%d): %v\n", ts(), code, err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Printf("[%s] DIALED %s\n", ts(), endpoint)

	auth, _ := json.Marshal(map[string]string{"userId": userID, "orderToken": token})
	if err := c.WriteMessage(websocket.TextMessage, auth); err != nil {
		fmt.Printf("[%s] AUTH WRITE FAILED: %v\n", ts(), err)
		os.Exit(1)
	}
	fmt.Printf("[%s] SENT auth {userId:%s}\n", ts(), userID)

	// Heartbeat every 30s to stay alive.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for range t.C {
			_ = c.WriteMessage(websocket.TextMessage, []byte(`{"heartbeat":"h"}`))
		}
	}()

	// Reader.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				fmt.Printf("[%s] READ ERR: %v\n", ts(), err)
				return
			}
			fmt.Printf("[%s] FRAME %s\n", ts(), string(msg))
		}
	}()

	select {
	case <-time.After(time.Duration(secs) * time.Second):
		fmt.Printf("[%s] window elapsed (%ds) — closing\n", ts(), secs)
	case <-done:
	}
}
