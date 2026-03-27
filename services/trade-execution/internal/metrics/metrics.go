// Package metrics provides Prometheus instrumentation for the trade-execution service.
//
// Usage: call metrics.Register() once at startup, then use the exported
// counters / histograms / gauges throughout the service.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ── Order execution ─────────────────────────────────────────────────────────

var (
	// OrdersTotal counts every order that enters ExecuteOrder, partitioned by
	// status outcome (submitted, rejected, failed) and mode (live, paper).
	OrdersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "orders_total",
		Help:      "Total orders processed, by final status and trading mode.",
	}, []string{"status", "mode"})

	// OrderLatency measures the wall-clock time of ExecuteOrder (broker round-trip
	// included). "mode" = live | paper.
	OrderLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "trade_execution",
		Name:      "order_latency_seconds",
		Help:      "End-to-end latency of order execution (includes retries).",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"mode"})

	// BrokerRetries counts individual retry attempts (excludes the first try).
	BrokerRetries = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "broker_retries_total",
		Help:      "Number of order placement retries to the broker.",
	})

	// BrokerErrors counts broker-level errors by category.
	BrokerErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "broker_errors_total",
		Help:      "Broker errors by category (timeout, auth, business, network).",
	}, []string{"category"})
)

// ── Fill rate ────────────────────────────────────────────────────────────────

var (
	// FillsTotal tracks filled orders (FILLED status from broker WS).
	FillsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "fills_total",
		Help:      "Total orders filled, by mode.",
	}, []string{"mode"})

	// RejectionsTotal tracks rejected / cancelled orders from broker.
	RejectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "rejections_total",
		Help:      "Total orders rejected or cancelled by broker.",
	}, []string{"reason"})
)

// ── Kafka consumer ──────────────────────────────────────────────────────────

var (
	// KafkaMessagesReceived counts messages fetched from each topic.
	KafkaMessagesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "kafka_messages_received_total",
		Help:      "Total Kafka messages received, by topic.",
	}, []string{"topic"})

	// KafkaProcessingErrors counts messages that failed processing.
	KafkaProcessingErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "kafka_processing_errors_total",
		Help:      "Kafka messages that failed processing, by topic.",
	}, []string{"topic"})

	// KafkaConsumerLag is the current lag (set on each message fetch).
	KafkaConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "trade_execution",
		Name:      "kafka_consumer_lag",
		Help:      "Estimated Kafka consumer lag (high-water - current offset).",
	}, []string{"topic", "partition"})
)

// ── WebSocket connections ───────────────────────────────────────────────────

var (
	// WSActiveConnections tracks currently connected WebSocket clients.
	WSActiveConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "trade_execution",
		Name:      "ws_active_connections",
		Help:      "Current number of active WebSocket connections, by type.",
	}, []string{"type"}) // type = paper | live

	// WSMessagesSent counts outbound WebSocket messages.
	WSMessagesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "ws_messages_sent_total",
		Help:      "Total WebSocket messages sent, by type and message_type.",
	}, []string{"ws_type", "message_type"})
)

// ── Status service (broker WS) ──────────────────────────────────────────────

var (
	// StatusUpdatesReceived counts order status updates from the Indira WS.
	StatusUpdatesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "status_updates_received_total",
		Help:      "Order status updates received from broker WS, by new_status.",
	}, []string{"status"})

	// BrokerWSReconnects counts reconnections to the shared Indira WS.
	BrokerWSReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "broker_ws_reconnects_total",
		Help:      "Number of broker WebSocket reconnection events.",
	})
)

// ── Price monitor ───────────────────────────────────────────────────────────

var (
	// PriceWatchesActive is the current count of orders being price-monitored.
	PriceWatchesActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "trade_execution",
		Name:      "price_watches_active",
		Help:      "Number of orders currently being price-monitored.",
	})

	// PriceTriggersTotal counts how many price-watch triggers fired.
	PriceTriggersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "trade_execution",
		Name:      "price_triggers_total",
		Help:      "Total price monitor triggers fired.",
	})
)

// ── Worker pool ─────────────────────────────────────────────────────────────

var (
	// WorkerPoolActive tracks goroutines currently processing a task.
	WorkerPoolActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "trade_execution",
		Name:      "worker_pool_active",
		Help:      "Number of worker pool goroutines currently processing.",
	})

	// WorkerPoolQueued tracks tasks waiting in the worker pool buffer.
	WorkerPoolQueued = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "trade_execution",
		Name:      "worker_pool_queued",
		Help:      "Number of tasks queued in the worker pool buffer.",
	})
)

// ── Component timing (signal flow) ─────────────────────────────────────────
// These histograms measure how long each component takes in the hot path,
// from signal arrival to broker submission.

var (
	// SignalKafkaDelay measures the time between Kafka message timestamp
	// and the moment we start processing it (queue + pool wait time).
	SignalKafkaDelay = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "trade_execution",
		Name:      "signal_kafka_delay_seconds",
		Help:      "Delay from Kafka message timestamp to processing start.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5},
	})

	// SignalDBPersistDuration measures order creation (INSERT) latency.
	SignalDBPersistDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "trade_execution",
		Name:      "signal_db_persist_seconds",
		Help:      "Time to persist a new order to PostgreSQL.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	})

	// SignalRouteDuration measures time in SignalProcessor routing, labeled by route.
	SignalRouteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "trade_execution",
		Name:      "signal_route_seconds",
		Help:      "Time from signal processing start to handoff to route (oco, price_monitor, executor).",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"route"})

	// BrokerAPICallDuration measures a single PlaceOrder API call (no retries).
	BrokerAPICallDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "trade_execution",
		Name:      "broker_api_call_seconds",
		Help:      "Duration of a single broker PlaceOrder API call (excludes retries).",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})

	// CredentialLookupDuration measures credential fetch time.
	CredentialLookupDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "trade_execution",
		Name:      "credential_lookup_seconds",
		Help:      "Time to resolve broker credentials (signal, cache, or db).",
		Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
	}, []string{"source"}) // source = "signal" | "cache" | "db"

	// SignalEndToEndDuration measures total time from Kafka message timestamp
	// to order submitted to broker (or routed to price monitor / OCO).
	SignalEndToEndDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "trade_execution",
		Name:      "signal_end_to_end_seconds",
		Help:      "Total time from Kafka message creation to order fully processed.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"route"})

	// StatusUpdateProcessDuration measures time to process a broker WS status update.
	StatusUpdateProcessDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "trade_execution",
		Name:      "status_update_process_seconds",
		Help:      "Time to process a single broker WS status update (DB lookup + update + publish).",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	})
)
