// brokerprobe — read-only operator tool: prints a user's live broker holdings
// as trade-execution sees them (GetBrokerHoldings over gRPC, the same call the
// positions reconciler makes). Nothing is placed, cancelled or modified, and
// no credential leaves trade-execution.
//
//	go run ./services/trade-execution/cmd/brokerprobe -addr localhost:9004 -user FIV99
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
)

func main() {
	addr := flag.String("addr", "localhost:9004", "trade-execution gRPC address")
	user := flag.String("user", "", "broker client id (user_id)")
	flag.Parse()
	if *user == "" {
		fmt.Fprintln(os.Stderr, "-user is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer conn.Close()
	resp, err := pb.NewTradeExecutionServiceClient(conn).GetBrokerHoldings(ctx, &pb.GetBrokerHoldingsRequest{UserId: *user})
	if err != nil {
		fmt.Fprintln(os.Stderr, "rpc:", err)
		os.Exit(1)
	}
	if !resp.GetSuccess() {
		fmt.Printf("user=%s broker fetch FAILED: %s %s\n", *user, resp.GetError().GetCode(), resp.GetError().GetMessage())
		os.Exit(3)
	}
	h := resp.GetHoldings()
	syms := make([]string, 0, len(h))
	for s := range h {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	fmt.Printf("user=%s holdings=%d symbols (delivery net qty, freeQty-safe)\n", *user, len(h))
	for _, s := range syms {
		fmt.Printf("  %-14s %d\n", s, h[s])
	}
}
