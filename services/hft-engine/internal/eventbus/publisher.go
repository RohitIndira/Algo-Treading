// Package eventbus publishes hft-engine audit events to a Redis pub/sub
// channel so the gateway can stream a live order/fill tape to the frontend.
//
// Design: fire-and-forget. A slow or down Redis must NEVER back-pressure
// the strategy goroutine, so Publish() is non-blocking (drops on a full
// buffer) and a drain goroutine does the actual Redis PUBLISH. The durable
// record is still the hft_audit_orders DB row — this stream is best-effort
// UI sugar.
package eventbus

import (
	"context"
	"encoding/json"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/repo"
)

// ChannelPrefix — events for strategy X are published to "hft:events:X".
// The gateway WS handler SUBSCRIBEs to exactly that per-strategy channel.
const ChannelPrefix = "hft:events:"

// event is the JSON shape pushed to the browser tape. Mirrors the audit
// row, trimmed to what a live tape needs.
type event struct {
	StrategyID    string  `json:"strategy_id"`
	TsUnixMs      int64   `json:"ts"`
	Action        string  `json:"action"` // PLACE|MODIFY|CANCEL|FILL|REJECT|ARM|PAUSE|RESUME|FILL_RECONCILED
	Side          string  `json:"side"`   // "B" | "S"
	ChunkSeq      int     `json:"chunk_seq"`
	Qty           int     `json:"qty"`
	Price         float64 `json:"price"`
	BrokerOrderID string  `json:"broker_order_id,omitempty"`
	BrokerStatus  string  `json:"broker_status,omitempty"`
	Error         string  `json:"error,omitempty"`
}

// Publisher drains audit events into Redis pub/sub.
type Publisher struct {
	rdb    *goredis.Client
	logger *zap.Logger
	ch     chan repo.AuditRow
	stop   chan struct{}
	done   chan struct{}
}

// NewPublisher wires a Redis client. Does not start the worker — call Start.
func NewPublisher(rdb *goredis.Client, logger *zap.Logger) *Publisher {
	return &Publisher{
		rdb:    rdb,
		logger: logger.Named("eventbus"),
		ch:     make(chan repo.AuditRow, 1024),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Start runs the drain goroutine until Stop() or ctx cancellation.
func (p *Publisher) Start(ctx context.Context) { go p.run(ctx) }

// Publish is the hot-path API — NEVER blocks. Drops the event if the
// buffer is full (the DB audit row remains the durable record).
// Satisfies audit.EventSink.
func (p *Publisher) Publish(e repo.AuditRow) {
	select {
	case p.ch <- e:
	default:
		// buffer full — drop; the tape is best-effort
	}
}

// Stop drains nothing extra; just signals the worker and waits.
func (p *Publisher) Stop() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	<-p.done
}

func (p *Publisher) run(ctx context.Context) {
	defer close(p.done)
	for {
		select {
		case <-p.stop:
			return
		case <-ctx.Done():
			return
		case e := <-p.ch:
			p.emit(ctx, e)
		}
	}
}

func (p *Publisher) emit(ctx context.Context, e repo.AuditRow) {
	ts := e.CreatedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	blob, err := json.Marshal(event{
		StrategyID:    e.StrategyID,
		TsUnixMs:      ts.UnixMilli(),
		Action:        e.Action,
		Side:          e.Side,
		ChunkSeq:      e.ChunkSeq,
		Qty:           e.Qty,
		Price:         e.Price,
		BrokerOrderID: e.BrokerOrderID,
		BrokerStatus:  e.BrokerStatus,
		Error:         e.ErrorMsg,
	})
	if err != nil {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := p.rdb.Publish(cctx, ChannelPrefix+e.StrategyID, blob).Err(); err != nil {
		p.logger.Warn("event publish failed",
			zap.String("strategy_id", e.StrategyID), zap.Error(err))
	}
}
