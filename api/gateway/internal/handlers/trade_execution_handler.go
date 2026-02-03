package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sort"
	"time"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/grpc_clients"
	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/encoding/protojson"
)

// TradeExecutionHandler exposes HTTP endpoints related to trade executions.
type TradeExecutionHandler struct {
	client *grpc_clients.TradeExecutionClient
}

func NewTradeExecutionHandler(client *grpc_clients.TradeExecutionClient) *TradeExecutionHandler {
	return &TradeExecutionHandler{client: client}
}

// ListSuccessfulUserOrders handles:
//
//	GET /api/v1/users/{user_id}/orders/success
//
// It returns *real* executed orders from the trade-execution service.
//
// Query params:
//  - page (default 1)
//  - page_size (default 20)
func (h *TradeExecutionHandler) ListSuccessfulUserOrders(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

	// Trading mode selector:
	// - LIVE (default) -> trade-execution gRPC
	// - PAPER -> read simulated fills from Kafka paper-executions.52w
	tradingMode := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("trading_mode")))
	if tradingMode == "" {
		tradingMode = strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("mode")))
	}

	// Parse pagination
	page := int32(1)
	pageSize := int32(20)
	if v := r.URL.Query().Get("page"); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 {
			page = int32(p)
		}
	}
	if v := r.URL.Query().Get("page_size"); v != "" {
		if ps, err := strconv.Atoi(v); err == nil && ps > 0 {
			pageSize = int32(ps)
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if tradingMode == "PAPER" {
		h.listPaperExecutionOrders(ctx, w, userID, page, pageSize)
		return
	}

	if h.client == nil {
		respondWithError(w, http.StatusInternalServerError, "trade-execution client not configured")
		return
	}

	grpcResp, err := h.client.GetUserOrders(ctx, &pb.GetUserOrdersRequest{
		UserId: userID,
		Filter: &pb.OrderFilter{
			Statuses: []common.OrderStatus{
				common.OrderStatus_ORDER_STATUS_FILLED,
				common.OrderStatus_ORDER_STATUS_PARTIAL_FILLED,
			},
		},
		Pagination: &common.PaginationRequest{
			Page:     page,
			PageSize: pageSize,
		},
	})
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to fetch orders: "+err.Error())
		return
	}
	if grpcResp == nil || !grpcResp.Success {
		errMsg := "Failed to fetch orders"
		if grpcResp != nil && grpcResp.Error != nil {
			errMsg = grpcResp.Error.Message
		}
		respondWithError(w, http.StatusBadRequest, errMsg)
		return
	}

	// Keep the gateway response shape stable for frontend:
	// { success, orders: [...], pagination: {...} }
	mo := protojson.MarshalOptions{EmitUnpopulated: true, UseEnumNumbers: false}
	orders := make([]json.RawMessage, 0, len(grpcResp.Orders))
	for _, o := range grpcResp.Orders {
		if o == nil {
			continue
		}
		b, err := mo.Marshal(o)
		if err != nil {
			continue
		}
		orders = append(orders, json.RawMessage(b))
	}

	var pagination json.RawMessage
	if grpcResp.Pagination != nil {
		b, err := mo.Marshal(grpcResp.Pagination)
		if err == nil {
			pagination = json.RawMessage(b)
		}
	}

	respondWithJSON(w, http.StatusOK, map[string]any{
		"success":    true,
		"user_id":    userID,
		"trading_mode": "LIVE",
		"orders":     orders,
		"pagination": pagination,
		"ts":         time.Now().Unix(),
	})
}

// listPaperExecutionOrders reads simulated executions from Kafka topic paper-executions.52w.
//
// NOTE: This is intentionally a "best effort" query layer for UI.
// Kafka is not a database, so we scan a recent window per partition.
func (h *TradeExecutionHandler) listPaperExecutionOrders(ctx context.Context, w http.ResponseWriter, userID string, page int32, pageSize int32) {
	brokersStr := os.Getenv("KAFKA_BROKERS")
	if brokersStr == "" {
		brokersStr = "localhost:9092"
	}
	parts := strings.Split(brokersStr, ",")
	brokers := make([]string, 0, len(parts))
	for _, b := range parts {
		b = strings.TrimSpace(b)
		if b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		respondWithError(w, http.StatusInternalServerError, "No Kafka brokers configured")
		return
	}

	topic := os.Getenv("KAFKA_TOPIC_PAPER_EXECUTIONS")
	if topic == "" {
		topic = "paper-executions.52w"
	}

	// How many messages per partition to scan from the tail.
	scanWindow := int64(5000)
	if v := os.Getenv("PAPER_ORDERS_SCAN_WINDOW"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			scanWindow = n
		}
	}

	// Discover partitions.
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to dial Kafka: "+err.Error())
		return
	}
	partsMeta, err := conn.ReadPartitions(topic)
	_ = conn.Close()
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to read Kafka partitions: "+err.Error())
		return
	}

	// Collect events for the requested user.
	events := make([]map[string]any, 0)
	partitionsScanned := make([]int, 0, len(partsMeta))

	for _, pm := range partsMeta {
		pid := pm.ID
		partitionsScanned = append(partitionsScanned, pid)

		// IMPORTANT:
		// We must know the partition "high watermark" (last offset) so we can
		// stop reading this partition. Otherwise ReadMessage() will block at the
		// end of the partition, and we will never move on to the next partition.
		leaderConn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, pid)
		if err != nil {
			continue
		}
		lastOffset, err := leaderConn.ReadLastOffset()
		_ = leaderConn.Close()
		if err != nil {
			continue
		}
		if lastOffset <= 0 {
			continue
		}

		// Read from the beginning so UI can show all paper orders.
		startOffset := kafka.FirstOffset
		_ = scanWindow // reserved for future tail scanning

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			Partition:   pid,
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     250 * time.Millisecond,
			StartOffset: startOffset,
		})

		// safety cap per partition to prevent infinite reads on hot topics
		maxReads := int64(200000)
		for reads := int64(0); reads < maxReads; reads++ {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				break
			}

			var ev map[string]any
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				continue
			}
			evUserID, _ := ev["user_id"].(string)
			if strings.TrimSpace(evUserID) != userID {
				continue
			}

			ev["_kafka_topic"] = topic
			ev["_kafka_partition"] = pid
			ev["_kafka_offset"] = msg.Offset
			events = append(events, ev)

			// Stop once we've consumed the last existing message (high watermark - 1)
			if msg.Offset >= lastOffset-1 {
				break
			}
		}
		_ = reader.Close()
	}

	// Sort by created_at desc if present.
	sort.SliceStable(events, func(i, j int) bool {
		ai, _ := events[i]["created_at"].(string)
		aj, _ := events[j]["created_at"].(string)
		ti, err1 := time.Parse(time.RFC3339Nano, ai)
		tj, err2 := time.Parse(time.RFC3339Nano, aj)
		if err1 == nil && err2 == nil {
			return ti.After(tj)
		}
		// fallback: offset desc
		oi, _ := events[i]["_kafka_offset"].(int64)
		oj, _ := events[j]["_kafka_offset"].(int64)
		return oi > oj
	})

	// Paginate
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	start := int((page - 1) * pageSize)
	end := start + int(pageSize)
	if start > len(events) {
		start = len(events)
	}
	if end > len(events) {
		end = len(events)
	}
	pageItems := events[start:end]

	totalItems := int64(len(events))
	totalPages := int32(0)
	if pageSize > 0 {
		totalPages = int32((totalItems + int64(pageSize) - 1) / int64(pageSize))
	}

	respondWithJSON(w, http.StatusOK, map[string]any{
		"success":      true,
		"user_id":      userID,
		"trading_mode": "PAPER",
		"orders":       pageItems,
		"partitions":   partitionsScanned,
		"pagination": map[string]any{
			"page":         page,
			"page_size":    pageSize,
			"total_items":  totalItems,
			"total_pages":  totalPages,
			"has_next":     page < totalPages,
			"has_previous": page > 1,
		},
		"topic": topic,
		"ts":    time.Now().Unix(),
	})
}
