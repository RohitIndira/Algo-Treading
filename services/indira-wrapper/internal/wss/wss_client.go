package wss

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	proto "github.com/RohitIndira/Algo-Treading/api/proto/indira_wrapper"
	"github.com/gorilla/websocket"
)

// BrokerOrderEvent is the raw JSON structure we expect from broker WSS.
// Field names are best-effort guesses based on REST response shapes.
// Update json tags once the real broker WSS schema is confirmed.
type BrokerOrderEvent struct {
	OrdId       string  `json:"ordId"`
	Status      string  `json:"status"`
	TradedQty   int32   `json:"tradedQty"`
	TradedPrice float64 `json:"tradedPrice"`
	RejReason   string  `json:"rejReason"`
}

type WSSStats struct {
	connected   atomic.Bool
	eventsRx    atomic.Int64
	lastEventNs atomic.Int64
	statusMsg   atomic.Value // string
}

type PollOrder struct {
	OrdId     string
	Status    string
	TradedQty int32
	Price     float64
	RejReason string
}

type Config struct {
	WSSURL         string
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	EventBufSize   int
	PollFn         func(ctx context.Context) ([]PollOrder, error)
}

type Client struct {
	wssURL string

	eventCh chan *proto.OrderEvent
	stats   WSSStats

	mu   sync.Mutex
	conn *websocket.Conn

	pollFn func(ctx context.Context) ([]PollOrder, error)

	initialBackoff time.Duration
	maxBackoff     time.Duration
}

func NewClient(cfg Config) *Client {
	ch := make(chan *proto.OrderEvent, cfg.EventBufSize)
	c := &Client{
		wssURL:         cfg.WSSURL,
		eventCh:        ch,
		pollFn:         cfg.PollFn,
		initialBackoff: cfg.InitialBackoff,
		maxBackoff:     cfg.MaxBackoff,
	}
	c.stats.statusMsg.Store("starting")
	return c
}

func (c *Client) EventCh() <-chan *proto.OrderEvent { return c.eventCh }

func (c *Client) Stats() (connected bool, statusMsg string, lastEventNs int64, eventsRx int64) {
	v := c.stats.statusMsg.Load()
	msg, _ := v.(string)
	return c.stats.connected.Load(), msg, c.stats.lastEventNs.Load(), c.stats.eventsRx.Load()
}

func (c *Client) Start(ctx context.Context) {
	if c.wssURL != "" {
		log.Printf("[WSS] mode=LIVE url=%s", c.wssURL)
		c.runWSSLoop(ctx)
		return
	}
	log.Printf("[WSS] mode=POLLING_FALLBACK (INDIRA_WSS_URL not set)")
	c.runPollLoop(ctx)
}

func (c *Client) runWSSLoop(ctx context.Context) {
	backoff := c.initialBackoff
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		c.stats.statusMsg.Store("connecting")
		if err := c.connect(ctx); err != nil {
			c.stats.connected.Store(false)
			c.stats.statusMsg.Store("connect_failed: " + err.Error())
			log.Printf("[WSS] connect failed: %v — retry in %v", err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < c.maxBackoff {
				backoff *= 2
				if backoff > c.maxBackoff {
					backoff = c.maxBackoff
				}
			}
			continue
		}

		backoff = c.initialBackoff
		c.stats.connected.Store(true)
		c.stats.statusMsg.Store("connected")
		log.Printf("[WSS] connected")

		c.readLoop(ctx)

		c.stats.connected.Store(false)
		c.stats.statusMsg.Store("disconnected — reconnecting")
	}
}

func (c *Client) connect(ctx context.Context) error {
	headers := http.Header{}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, c.wssURL, headers)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

func (c *Client) readLoop(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		if c.conn != nil {
			_ = c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("[WSS] read error: %v", err)
			return
		}

		evt, err := c.parseWSSEvent(msg)
		if err != nil {
			log.Printf("[WSS] parse error: %v raw=%s", err, string(msg))
			continue
		}
		if evt == nil {
			continue
		}
		c.pushEvent(evt)
	}
}

func (c *Client) parseWSSEvent(msg []byte) (*proto.OrderEvent, error) {
	var raw BrokerOrderEvent
	if err := json.Unmarshal(msg, &raw); err != nil {
		return nil, err
	}
	if raw.OrdId == "" {
		return nil, nil
	}

	eventType := mapStatusToEventType(raw.Status)
	if eventType == "" {
		return nil, nil
	}

	now := time.Now().UnixNano()
	c.stats.lastEventNs.Store(now)

	return &proto.OrderEvent{
		BrokerOrderId: raw.OrdId,
		EventType:     eventType,
		RawStatus:     raw.Status,
		TradedQty:     raw.TradedQty,
		TradedPrice:   raw.TradedPrice,
		RejReason:     raw.RejReason,
		TimestampNs:   now,
		RawPayload:    string(msg),
	}, nil
}

func (c *Client) runPollLoop(ctx context.Context) {
	c.stats.statusMsg.Store("polling_fallback")
	c.stats.connected.Store(true)

	prev := make(map[string]PollOrder)
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c.pollFn == nil {
				continue
			}
			orders, err := c.pollFn(ctx)
			if err != nil {
				log.Printf("[WSS-POLL] poll error: %v", err)
				continue
			}
			for _, o := range orders {
				p, ok := prev[o.OrdId]
				if !ok || p.Status != o.Status || p.TradedQty != o.TradedQty {
					now := time.Now().UnixNano()
					c.pushEvent(&proto.OrderEvent{
						BrokerOrderId: o.OrdId,
						EventType:     mapStatusToEventType(o.Status),
						RawStatus:     o.Status,
						TradedQty:     o.TradedQty,
						TradedPrice:   o.Price,
						RejReason:     o.RejReason,
						TimestampNs:   now,
					})
					c.stats.lastEventNs.Store(now)
				}
				prev[o.OrdId] = o
			}
		}
	}
}

func (c *Client) pushEvent(evt *proto.OrderEvent) {
	select {
	case c.eventCh <- evt:
		c.stats.eventsRx.Add(1)
	default:
		log.Printf("[WSS] WARNING: event channel full, dropping broker_order_id=%s event_type=%s", evt.BrokerOrderId, evt.EventType)
	}
}

func mapStatusToEventType(status string) string {
	switch status {
	case "Executed":
		return "ORDER_FILLED"
	case "Rejected":
		return "ORDER_REJECTED"
	case "Cancelled":
		return "ORDER_CANCELLED"
	case "Requested", "Pending":
		return "ORDER_PLACED"
	default:
		return ""
	}
}
