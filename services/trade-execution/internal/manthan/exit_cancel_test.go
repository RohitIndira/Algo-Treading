package manthan

import (
	"testing"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

func todayIST(h, m int) string {
	ist, _ := time.LoadLocation("Asia/Kolkata")
	n := time.Now().In(ist)
	return time.Date(n.Year(), n.Month(), n.Day(), h, m, 0, 0, ist).Format("2006-01-02 15:04:05")
}

// An AMO exit gets a NEW broker id at the 08:50 conversion. The matcher must
// find exactly that order (same symbol / SELL / Limit / qty / price, today)
// and refuse anything ambiguous or foreign.
func TestMatchConvertedExit(t *testing.T) {
	row := &ManthanOrder{Symbol: "MODISONLTD", Qty: 51, LimitPrice: 428.10, BrokerOrderID: "NYMZX00027F:", OrderType: OrderTypeAMOSell}
	sym := indiraClient.OrderBookSymbol{BaseSym: "MODISONLTD"}
	mk := func(id, action, otype string, qty int, price float64, date string) indiraClient.OrderBook {
		return indiraClient.OrderBook{OrdId: id, OrdAction: action, OrdType: otype, Symbol: sym, Qty: qty, Price: price, OrdDate: date, Status: "Pending"}
	}
	converted := mk("NYMZX00099F:", "SELL", "Limit", 51, 428.10, todayIST(8, 50))

	cases := []struct {
		name string
		book []indiraClient.OrderBook
		want string
	}{
		{"finds the converted order", []indiraClient.OrderBook{converted}, "NYMZX00099F:"},
		{"ignores the stale original id", []indiraClient.OrderBook{mk("NYMZX00027F:", "SELL", "Limit", 51, 428.10, todayIST(8, 50)), converted}, "NYMZX00099F:"},
		{"wrong qty", []indiraClient.OrderBook{mk("X1", "SELL", "Limit", 50, 428.10, todayIST(8, 50))}, ""},
		{"wrong price", []indiraClient.OrderBook{mk("X1", "SELL", "Limit", 51, 430.00, todayIST(8, 50))}, ""},
		{"a stop, not a limit", []indiraClient.OrderBook{mk("X1", "SELL", "Stop-loss", 51, 428.10, todayIST(8, 50))}, ""},
		{"a buy", []indiraClient.OrderBook{mk("X1", "BUY", "Limit", 51, 428.10, todayIST(8, 50))}, ""},
		{"yesterday's order", []indiraClient.OrderBook{mk("X1", "SELL", "Limit", 51, 428.10, "2026-10-21 09:15:00")}, ""},
		{"ambiguous → nil", []indiraClient.OrderBook{converted, mk("X2", "SELL", "Limit", 51, 428.10, todayIST(8, 51))}, ""},
	}
	for _, c := range cases {
		got := matchConvertedExit(row, c.book)
		gotID := ""
		if got != nil {
			gotID = got.OrdId
		}
		if gotID != c.want {
			t.Errorf("%s: got %q want %q", c.name, gotID, c.want)
		}
	}
}
