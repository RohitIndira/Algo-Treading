package consumer

import (
	"context"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/engine"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// EngineHandler is the new rules-engine hot-path handler.
//
// It is intentionally small and does NOT publish signals itself.
// Publishing is done via the callback injected from cmd/main.go.
type EngineHandler struct {
	eng      *engine.Engine
	onSignal func(ctx context.Context, match *models.RuleMatch, event *models.MarketEvent)
}

func NewEngineHandler(eng *engine.Engine, onSignal func(ctx context.Context, match *models.RuleMatch, event *models.MarketEvent)) *EngineHandler {
	return &EngineHandler{eng: eng, onSignal: onSignal}
}

func (h *EngineHandler) HandleEvent(ctx context.Context, event *models.MarketEvent) error {
	matches, err := h.eng.EvaluateEvent(ctx, event)
	if err != nil {
		return err
	}
	for _, m := range matches {
		if m == nil {
			continue
		}
		// callback must be fast and non-blocking; it must not break ordering.
		h.onSignal(ctx, m, event)
	}
	_ = time.Now()
	return nil
}
