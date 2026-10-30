package refdata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(srv *httptest.Server) *Client {
	c := NewClient(srv.URL, "test-key")
	c.sleep = func(time.Duration) {}
	return c
}

// Pages are walked 1..total_pages and the x-api-key header is sent.
func TestEOD_PagesAndHeader(t *testing.T) {
	var sawKey atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") == "test-key" {
			sawKey.Store(true)
		}
		if r.URL.Path != "/equities/eod" {
			http.NotFound(w, r)
			return
		}
		page := r.URL.Query().Get("page")
		pageN, _ := strconv.Atoi(page)
		rows := []map[string]any{}
		switch page {
		case "1":
			rows = append(rows, map[string]any{"trade_date": "2026-10-29", "isin": "INE1", "nse_symbol": "AAA", "company_name": "A", "close_price": 10.5, "market_cap_cr": 1200.0})
			rows = append(rows, map[string]any{"trade_date": "2026-10-29", "isin": "INE2", "nse_symbol": nil, "company_name": "B", "close_price": 20, "market_cap_cr": nil})
		case "2":
			rows = append(rows, map[string]any{"trade_date": "2026-10-29", "isin": "INE3", "nse_symbol": "CCC", "company_name": "C", "close_price": 30})
		}
		json.NewEncoder(w).Encode(map[string]any{
			"success": true, "data": rows,
			"meta": map[string]any{"page": pageN, "limit": 1000, "total": 3, "total_pages": 2},
		})
	}))
	defer srv.Close()

	rows, err := newTestClient(srv).EOD(context.Background(), "2026-10-29")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].ISIN != "INE1" || rows[2].ISIN != "INE3" {
		t.Fatalf("rows: %+v", rows)
	}
	if rows[1].NSESymbol != nil || rows[1].MarketCapCr != nil {
		t.Fatalf("null fields must decode to nil: %+v", rows[1])
	}
	if !sawKey.Load() {
		t.Fatal("x-api-key header not sent")
	}
}

// A 429 with Retry-After is retried; a 404 becomes NotFoundError.
func TestGet_RateLimitAndNotFound(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/equities/fundamentals":
			if atomic.AddInt32(&calls, 1) == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data":    []map[string]any{{"period": "2026-06", "isin": "INE1", "pat_cr": 187, "pat_basis": "standalone", "f_score": 66}},
				"meta":    map[string]any{"page": 1, "limit": 1000, "total": 1, "total_pages": 1},
			})
		case "/equities/eod":
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"success":false,"message":"No data for 2026-10-26"}`)
		}
	}))
	defer srv.Close()
	c := newTestClient(srv)

	rows, err := c.Fundamentals(context.Background(), "2026-06", "preferred")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].PATCr == nil || *rows[0].PATCr != 187 || *rows[0].PATBasis != "standalone" {
		t.Fatalf("rows: %+v", rows)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expected one retry after 429, got %d calls", calls)
	}

	_, err = c.EOD(context.Background(), "2026-10-26")
	var nf *NotFoundError
	if err == nil || !asNotFound(err, &nf) || nf.Msg != "No data for 2026-10-26" {
		t.Fatalf("want NotFoundError, got %v", err)
	}
}

// price-levels has no total_pages: stop on a short page or when total reached.
func TestPriceLevels_StopsOnShortPage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		pageN, _ := strconv.Atoi(page)
		mk := func(n int, off int) []map[string]any {
			out := make([]map[string]any, 0, n)
			for i := 0; i < n; i++ {
				out = append(out, map[string]any{"co_code": off + i, "symbol": fmt.Sprintf("S%d", off+i), "company_name": "x",
					"week_52_high": 10.0, "week_52_low": 5.0, "all_time_high": 12.0})
			}
			return out
		}
		var data []map[string]any
		switch page {
		case "1":
			data = mk(200, 0)
		case "2":
			data = mk(50, 200)
		default:
			t.Errorf("unexpected page %s", page)
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "count": len(data), "page": pageN, "limit": 200, "total": 250, "data": data})
	}))
	defer srv.Close()
	rows, err := newTestClient(srv).PriceLevels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 250 || rows[249].Symbol != "S249" {
		t.Fatalf("rows=%d", len(rows))
	}
}

func asNotFound(err error, target **NotFoundError) bool {
	nf, ok := err.(*NotFoundError)
	if ok {
		*target = nf
	}
	return ok
}
