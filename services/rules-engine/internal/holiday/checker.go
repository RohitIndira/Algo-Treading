package holiday

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

// odinDocument represents a document from the OdinMasterData collection.
type odinDocument struct {
	OuterKey string   `bson:"outer_key"`
	SubKey   string   `bson:"sub_key"`
	Values   []string `bson:"values"`
}

// Checker loads trading holidays from MongoDB (OdinMasterData) and checks
// whether a given date is a holiday for BSE or NSE equity segments.
type Checker struct {
	client     *mongo.Client
	collection *mongo.Collection
	logger     *zap.Logger
	timezone   *time.Location

	mu       sync.RWMutex
	holidays map[string]struct{} // "YYYY-MM-DD" → present means holiday
}

// Config holds configuration for the holiday checker.
type Config struct {
	MongoURI string
	Timezone string // e.g. "Asia/Kolkata"
}

// New connects to MongoDB, loads the initial holiday set, and returns
// a ready-to-use Checker. Call Close() when done.
func New(ctx context.Context, cfg Config, logger *zap.Logger) (*Checker, error) {
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}

	clientOpts := options.Client().ApplyURI(cfg.MongoURI)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("holiday checker: mongo connect: %w", err)
	}

	// Verify connectivity.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("holiday checker: mongo ping: %w", err)
	}

	c := &Checker{
		client:     client,
		collection: client.Database("OdinMasterData").Collection("HolidayMaster"),
		logger:     logger,
		timezone:   loc,
		holidays:   make(map[string]struct{}),
	}

	if err := c.Refresh(ctx); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("holiday checker: initial load: %w", err)
	}

	return c, nil
}

// Refresh reloads the holiday dates from MongoDB for sub_key BSE_EQ and NSE_EQ.
func (c *Checker) Refresh(ctx context.Context) error {
	filter := bson.M{
		"outer_key": "data",
		"sub_key":   bson.M{"$in": []string{"BSE_EQ", "NSE_EQ"}},
	}

	cursor, err := c.collection.Find(ctx, filter)
	if err != nil {
		return fmt.Errorf("holiday checker: query: %w", err)
	}
	defer cursor.Close(ctx)

	merged := make(map[string]struct{})
	var count int
	for cursor.Next(ctx) {
		var doc odinDocument
		if err := cursor.Decode(&doc); err != nil {
			c.logger.Warn("holiday checker: failed to decode document", zap.Error(err))
			continue
		}
		for _, d := range doc.Values {
			merged[d] = struct{}{}
		}
		count++
		c.logger.Info("Loaded trading holidays",
			zap.String("sub_key", doc.SubKey),
			zap.Int("count", len(doc.Values)))
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("holiday checker: cursor: %w", err)
	}

	if count == 0 {
		c.logger.Warn("holiday checker: no BSE_EQ/NSE_EQ holiday documents found in OdinMasterData")
	}

	c.mu.Lock()
	c.holidays = merged
	c.mu.Unlock()

	c.logger.Info("Trading holidays refreshed",
		zap.Int("total_holiday_dates", len(merged)),
		zap.Int("documents_loaded", count))
	return nil
}

// IsTodayHoliday returns true if today (in the configured timezone) is a
// trading holiday for either BSE or NSE equity.
func (c *Checker) IsTodayHoliday() bool {
	today := time.Now().In(c.timezone).Format("2006-01-02")
	c.mu.RLock()
	_, ok := c.holidays[today]
	c.mu.RUnlock()
	return ok
}

// IsDateHoliday checks whether a specific date string ("YYYY-MM-DD") is a holiday.
func (c *Checker) IsDateHoliday(date string) bool {
	c.mu.RLock()
	_, ok := c.holidays[date]
	c.mu.RUnlock()
	return ok
}

// NSEHolidays returns a snapshot copy of the loaded holiday set.
// Keys are "YYYY-MM-DD" strings. Used by the AMN backfill to compute the
// previous trading day without holding the checker's lock.
func (c *Checker) NSEHolidays() map[string]struct{} {
	c.mu.RLock()
	snap := make(map[string]struct{}, len(c.holidays))
	for k, v := range c.holidays {
		snap[k] = v
	}
	c.mu.RUnlock()
	return snap
}

// StartAutoRefresh spawns a goroutine that refreshes holidays once per day.
// It stops when ctx is cancelled.
func (c *Checker) StartAutoRefresh(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := c.Refresh(ctx); err != nil {
					c.logger.Error("holiday checker: auto-refresh failed", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Close disconnects the MongoDB client.
func (c *Checker) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}
