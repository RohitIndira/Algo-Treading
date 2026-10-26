package wss

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for the dealer broadcast listener. Registered once on the
// default registry (main.go serves it on /metrics). Label cardinality is
// bounded: `ucc` is a managed client id or "other"; `user` is the anchor id.
var (
	mBroadcastConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "broadcast_connected",
		Help: "1 while the dealer broadcast socket is connected and the handshake was accepted.",
	})
	mBroadcastLastFrameAge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "broadcast_last_frame_age_seconds",
		Help: "Seconds since the last order frame arrived on the broadcast socket (-1 before the first frame).",
	})
	mBroadcastFrames = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "broadcast_frames_total",
		Help: "Order frames received on the broadcast socket by client code (managed clients by id, everyone else as 'other').",
	}, []string{"ucc"})
	mBroadcastReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broadcast_reconnects_total",
		Help: "Broadcast socket (re)connect attempts after the first successful connection.",
	})
	mBroadcastUnknownUCC = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broadcast_unknown_ucc_total",
		Help: "Frames for client codes orderstatus does not manage (counted, never stored).",
	})
	mBroadcastAnchor = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "broadcast_anchor_session",
		Help: "1 for the user whose session currently anchors the broadcast feed.",
	}, []string{"user"})
	mBroadcastShadowRecall = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "broadcast_shadow_recall_total",
		Help: "Shadow mode: managed-client frames by whether the per-user socket / REST poll had stored the same (broker_order_id, event_seq) within the compare window.",
	}, []string{"seen_by_user_path"})
	mBroadcastShadowLead = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "broadcast_shadow_lead_seconds",
		Help:    "Shadow mode: per-user path observed_at minus broadcast arrival, for frames both paths saw. Positive = broadcast was earlier.",
		Buckets: []float64{-30, -10, -5, -2, -1, -0.5, 0, 0.5, 1, 2, 5, 10, 30},
	})
	mBroadcastWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "broadcast_written_total",
		Help: "Frames the broadcast listener handed to the store, by result (inserted / deduped / dropped / error).",
	}, []string{"result"})
	mBroadcastSkippedNotOwn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broadcast_skipped_not_own_total",
		Help: "own_orders mode: managed-client frames skipped because the broker id is not one of our orders (or the lookup failed).",
	})
	mBroadcastMalformed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broadcast_malformed_frames_total",
		Help: "Frames that were not valid JSON order-status objects.",
	})
	mBroadcastControl = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broadcast_control_frames_total",
		Help: "JSON frames without a UCC (handshake replies, server notices).",
	})
	mBroadcastTokenErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broadcast_token_errors_total",
		Help: "Failed createWsToken calls (any anchor).",
	})
	mBroadcastShadowSeqMatch = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "broadcast_shadow_seq_match_total",
		Help: "Shadow mode: per managed frame, 'seq' = per-user path stored the same (broker_order_id, event_seq); 'equivalent_only' = same event content stored under a different seq (primary dedup would NOT hold); 'none' = not seen by the per-user path.",
	}, []string{"match"})
	mBroadcastPartialParse = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broadcast_partial_parse_total",
		Help: "Order frames kept despite a JSON type mismatch on a non-routing field (broker flips number/string types).",
	})
	mBroadcastQueueDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broadcast_write_queue_dropped_total",
		Help: "own_orders / all: frames dropped because the bounded write queue (10000) was full.",
	})
	mBroadcastAnchorSwitches = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broadcast_anchor_switches_total",
		Help: "Times the listener had to give up on an anchor session and pick another.",
	})
)
