// odin-probe — standalone diagnostic for the ODIN broadcast feed.
//
// Purpose:
//   - Connect to ODIN over WSS using whatever creds (or no creds) you give.
//   - Subscribe to ONE (segment_id, security_code) pair.
//   - Print every parsed FIX message that arrives for N seconds.
//   - Exit cleanly.
//
// This does NOT touch Postgres, Redis, the broker, or the strategy engine.
// It's the minimal reproducer for "is the broadcast feed actually streaming
// bid/ask to us?" — useful both at first-deploy (verifying creds work)
// and any time the engine reports no_data halts.
//
// Examples:
//
//   # Plain probe — uses defaults from .env or built-in fallbacks.
//   go run ./services/hft-engine/cmd/odin-probe/
//
//   # Probe a specific stock for 60 seconds.
//   go run ./services/hft-engine/cmd/odin-probe/ \
//       --symbol ARVIND --security-code 193 --duration 60s
//
//   # Override creds (instead of relying on .env).
//   go run ./services/hft-engine/cmd/odin-probe/ \
//       --user-id YOUR_ID --api-key YOUR_KEY
//
// Common NSE Cash security codes for sanity-testing:
//   ARVIND     193
//   RELIANCE   2885
//   IDEA       14366
//   SBIN       3045
//   TATAMOTORS 3456
//
// All are segment_id=1 (NSE Cash).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/marketws"
)

func main() {
	// ── flags ────────────────────────────────────────────────────────────
	var (
		host     = flag.String("host", "", "ODIN broadcast host (defaults to env / built-in)")
		port     = flag.Int("port", 0, "ODIN broadcast port (defaults to env / built-in)")
		scheme   = flag.String("scheme", "", `"wss" or "ws" — defaults to env / "wss"`)
		userID   = flag.String("user-id", "", "ODIN broadcast user id (defaults to env)")
		apiKey   = flag.String("api-key", "", "ODIN broadcast api key (defaults to env)")
		segment  = flag.Int("segment", 1, "Market segment id — 1=NSE Cash, 2=NSE Derivatives, ...")
		secCode  = flag.Int64("security-code", 193, "Exchange token to subscribe to (default 193=ARVIND)")
		symbol   = flag.String("symbol", "ARVIND", "Human label for logs only — doesn't affect subscription")
		duration = flag.Duration("duration", 30*time.Second, "How long to listen before exiting")
		verbose  = flag.Bool("verbose", false, "Print EVERY FIX message, not just 209 touchlines")
	)
	flag.Parse()

	// ── env fallbacks (.env if present) ──────────────────────────────────
	// Same lookup order the engine uses, so the probe sees the same creds.
	for _, p := range []string{".env", filepath.Join("services", "hft-engine", ".env")} {
		if abs, err := filepath.Abs(p); err == nil {
			_ = godotenv.Overload(abs)
		}
	}
	if *host == "" {
		*host = getenv("ODIN_BCAST_HOST", "indirabcast.odin.trade")
	}
	if *port == 0 {
		*port = atoi(getenv("ODIN_BCAST_PORT", "4515"), 4515)
	}
	if *scheme == "" {
		*scheme = getenv("ODIN_BCAST_SCHEME", "wss")
	}
	if *userID == "" {
		*userID = os.Getenv("ODIN_BCAST_USER_ID")
	}
	if *apiKey == "" {
		*apiKey = os.Getenv("ODIN_BCAST_API_KEY")
	}

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	log.Printf("──────────────────────────────────────────────────────────")
	log.Printf(" odin-probe")
	log.Printf(" target:   %s://%s:%d", *scheme, *host, *port)
	log.Printf(" creds:    user_id=%q  api_key=%s", *userID, mask(*apiKey))
	log.Printf(" subscribe: segment=%d security_code=%d (%s)", *segment, *secCode, *symbol)
	log.Printf(" duration: %s", *duration)
	log.Printf("──────────────────────────────────────────────────────────")

	// ── build the client ─────────────────────────────────────────────────
	client := marketws.New(marketws.OdinClientConfig{
		Scheme: *scheme,
		Host:   *host,
		Port:   *port,
		UserID: *userID,
		APIKey: *apiKey,
	}, logger)

	// ── stats counters ───────────────────────────────────────────────────
	var (
		messages    int
		touchlines  int
		firstTickAt time.Time
		lastTickAt  time.Time
	)

	client.SetListener(func(msg marketws.FixMessage) {
		messages++
		now := time.Now()
		if firstTickAt.IsZero() {
			firstTickAt = now
		}
		lastTickAt = now

		code := msg.Code()
		if tl := msg.AsTouchline(); tl != nil {
			touchlines++
			fmt.Printf("[%-8s] 209 %s sec=%d  bid=%.2f x%-4d  ask=%.2f x%-4d  ltp=%.2f x%d\n",
				now.Format("15:04:05.000"),
				*symbol,
				tl.SecurityCode,
				float64(tl.BidPaise)/100, tl.BidQty,
				float64(tl.AskPaise)/100, tl.AskQty,
				float64(tl.LTPPaise)/100, tl.LTPQty,
			)
			return
		}
		if resp := msg.AsLogonResponse(); resp != nil {
			fmt.Printf("[%-8s] 102 logon  status=%d  segment=%d  days_to_expire=%d\n",
				now.Format("15:04:05.000"),
				resp.LogonStatus, resp.SegmentID, resp.DaysToExpire)
			return
		}
		if *verbose {
			fmt.Printf("[%-8s] %3d %v\n", now.Format("15:04:05.000"), code, summarize(msg))
		}
	})

	// ── run + subscribe + wait ───────────────────────────────────────────
	ctx, cancel := context.WithTimeout(context.Background(), *duration+10*time.Second)
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("interrupt — shutting down")
		cancel()
	}()

	// Run the client. Returns on ctx done or auth-rejected.
	clientDone := make(chan error, 1)
	go func() { clientDone <- client.Run(ctx) }()

	// Wait for login before subscribing.
	if err := waitForConnected(ctx, client, 5*time.Second); err != nil {
		log.Fatalf("never reached Connected state: %v", err)
	}

	if err := client.Subscribe(*segment, *secCode); err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}
	log.Printf("subscribed — listening for %s", *duration)

	// Block for duration or until client exits.
	select {
	case <-time.After(*duration):
		log.Println("duration elapsed — disconnecting")
		cancel()
	case err := <-clientDone:
		log.Printf("ODIN client exited early: %v", err)
	}

	// Drain.
	<-time.After(500 * time.Millisecond)
	log.Printf("──────────────────────────────────────────────────────────")
	log.Printf(" SUMMARY")
	log.Printf(" messages received: %d  (of which %d were 209 touchline)", messages, touchlines)
	if !firstTickAt.IsZero() {
		log.Printf(" first message:     %s", firstTickAt.Format("15:04:05.000"))
		log.Printf(" last message:      %s", lastTickAt.Format("15:04:05.000"))
		log.Printf(" stream-time:       %s", lastTickAt.Sub(firstTickAt).Round(time.Millisecond))
	}
	log.Printf("──────────────────────────────────────────────────────────")
	if touchlines == 0 {
		log.Println()
		log.Println("⚠  ZERO touchline (209) messages — the feed accepted our login but isn't streaming.")
		log.Println("   Likely causes:")
		log.Println("     1. ODIN_BCAST_USER_ID / API_KEY are empty or invalid — read-only login")
		log.Println("        succeeds but data subscription is silently denied.")
		log.Println("     2. Market is closed (no ticks fire 09:15-15:30 IST).")
		log.Println("     3. Wrong security_code for the symbol.")
		os.Exit(2)
	}
}

// ── helpers ─────────────────────────────────────────────────────────────

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func atoi(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

func mask(s string) string {
	if s == "" {
		return "(empty)"
	}
	if len(s) <= 4 {
		return "****"
	}
	return s[:2] + "****" + s[len(s)-2:]
}

func summarize(m marketws.FixMessage) string {
	out := ""
	for k, v := range m {
		if k == 64 || k == 65 || k == 66 {
			continue // header noise
		}
		out += fmt.Sprintf("%d=%s ", k, v)
		if len(out) > 80 {
			out += "..."
			break
		}
	}
	return out
}

func waitForConnected(ctx context.Context, c *marketws.OdinClient, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.State() == marketws.StateConnected {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out after %s", timeout)
}
