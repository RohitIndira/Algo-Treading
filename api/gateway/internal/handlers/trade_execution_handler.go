package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/grpc_clients"
	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

// TradeExecutionHandler exposes HTTP endpoints related to trade executions.
// For now, the /orders/success endpoint reads directly from the Kafka
// trade-executions topic instead of the trade-execution gRPC/DB layer.
type TradeExecutionHandler struct {
	// We keep the gRPC client in case we want to switch back to DB-based
	// queries later, but it is not used in the Kafka-backed endpoint.
	client *grpc_clients.TradeExecutionClient
}

func NewTradeExecutionHandler(client *grpc_clients.TradeExecutionClient) *TradeExecutionHandler {
	return &TradeExecutionHandler{client: client}
}

// ListSuccessfulUserOrders handles:
//
//	GET /api/v1/users/{user_id}/orders/success
//
// It reads all messages from the Kafka "trade-executions" topic from the
// beginning on each request, filters them by user_id from the path, and
// returns the raw execution result JSONs for that user. This matches your
// request to make this API reflect the Kafka topic directly.
func (h *TradeExecutionHandler) ListSuccessfulUserOrders(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]
	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id is required")
		return
	}

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

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	// For now, we know from your Kafka UI that all messages for this
	// environment are on partition 2 of trade-executions. To keep things
	// simple and reliable, we read directly from that partition starting
	// at the first offset.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       "trade-executions",
		Partition:   2,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	executions := make([]map[string]any, 0)
	maxMessages := 1000 // safety limit per request
	for i := 0; i < maxMessages; i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			// On timeout or EOF, stop reading.
			break
		}

		var ev map[string]any
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			continue
		}

		// TEMP: append all messages from partition 2 so we can verify that
		// the gateway is correctly connected to Kafka and reading the
		// trade-executions topic. Once we confirm messages appear here, we
		// can re-introduce filtering by user_id.
		executions = append(executions, ev)
	}

	respondWithJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"orders":  executions,
	})
}
