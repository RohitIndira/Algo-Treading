package consumer

import (
	"context"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// EventHandler defines the interface for handling events.
// Implemented by *Handler.
type EventHandler interface {
	HandleEvent(ctx context.Context, event *models.MarketEvent) error
}
