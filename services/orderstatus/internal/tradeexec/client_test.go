package tradeexec

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
)

type fakeRPC struct {
	pb.TradeExecutionServiceClient // unimplemented methods panic if called
	calls                          int
	resp                           *pb.LookupOrderMetaResponse
	err                            error
}

func (f *fakeRPC) LookupOrderMeta(context.Context, *pb.LookupOrderMetaRequest, ...grpc.CallOption) (*pb.LookupOrderMetaResponse, error) {
	f.calls++
	return f.resp, f.err
}

func TestIsOurOrder_ServiceErrorFailsClosedWithoutNegativeCache(t *testing.T) {
	rpc := &fakeRPC{resp: &pb.LookupOrderMetaResponse{Found: false, Error: &common.Error{Code: "DB_DOWN", Message: "boom"}}}
	c := newWithRPC(rpc)
	for i := 0; i < 2; i++ {
		ok, err := c.IsOurOrder(context.Background(), "X1")
		if ok || err == nil {
			t.Fatalf("call %d: ok=%v err=%v; want false + error", i, ok, err)
		}
	}
	if rpc.calls != 2 {
		t.Fatalf("service error must not be cached: calls=%d", rpc.calls)
	}
	if _, cached := c.negative["X1"]; cached {
		t.Fatal("service error landed in the negative cache")
	}
	rpc.err, rpc.resp = errors.New("unavailable"), nil
	if ok, err := c.IsOurOrder(context.Background(), "X2"); ok || err == nil {
		t.Fatalf("transport error: ok=%v err=%v", ok, err)
	}
}

func TestIsOurOrder_CachesAndEvictsNegative(t *testing.T) {
	rpc := &fakeRPC{resp: &pb.LookupOrderMetaResponse{Found: false}}
	c := newWithRPC(rpc)
	c.negativeTTL = 20 * time.Millisecond
	if ok, err := c.IsOurOrder(context.Background(), "M1"); ok || err != nil {
		t.Fatalf("first: %v %v", ok, err)
	}
	if ok, _ := c.IsOurOrder(context.Background(), "M1"); ok || rpc.calls != 1 {
		t.Fatalf("negative cache not used: calls=%d", rpc.calls)
	}
	time.Sleep(30 * time.Millisecond)
	rpc.resp = &pb.LookupOrderMetaResponse{Found: true}
	if ok, _ := c.IsOurOrder(context.Background(), "M1"); !ok || rpc.calls != 2 {
		t.Fatalf("expired negative entry must be re-asked and evicted: ok=%v calls=%d", ok, rpc.calls)
	}
	if _, still := c.negative["M1"]; still {
		t.Fatal("negative entry not evicted after positive answer")
	}
	// Positive stays for life.
	rpc.resp = &pb.LookupOrderMetaResponse{Found: false}
	if ok, _ := c.IsOurOrder(context.Background(), "M1"); !ok || rpc.calls != 2 {
		t.Fatalf("positive cache not used: ok=%v calls=%d", ok, rpc.calls)
	}
}
