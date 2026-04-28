package data

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/database/mongodb"
	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/models"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
)

// RedisManager manages Redis interactions and data loading
type RedisManager struct {
	client      *redis.Client
	lgr         *logger.Logger
	mongoClient *mongodb.Client // Store mongoClient for lazy loading
}



// NewRedisManager creates a new Redis manager
func NewRedisManager(uri, password string, db int, lgr *logger.Logger, mongoClient *mongodb.Client) (*RedisManager, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     uri,
		Password: password,
		DB:       db,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisManager{
		client:      rdb,
		lgr:         lgr,
		mongoClient: mongoClient,
	}, nil
}

// Close closes the Redis connection
func (rm *RedisManager) Close() error {
	return rm.client.Close()
}

// GetCompanyDetails retrieves company details from Redis, or fetches from MongoDB if missing
func (rm *RedisManager) GetCompanyDetails(ctx context.Context, isin string) (*models.CompanyDetails, error) {
	key := fmt.Sprintf("isin:%s", isin)
	val, err := rm.client.Get(ctx, key).Result()

	if err == redis.Nil {
		// Cache miss - fetch from MongoDB
		rm.lgr.Info("redis miss, fetching from mongodb", zap.String("isin", isin))
		return rm.fetchAndCacheCompanyDetails(ctx, isin)
	}
	if err != nil {
		return nil, err
	}

	var details models.CompanyDetails
	if err := json.Unmarshal([]byte(val), &details); err != nil {
		return nil, fmt.Errorf("failed to unmarshal company details: %w", err)
	}

	return &details, nil
}

// fetchAndCacheCompanyDetails queries MongoDB for the ISIN, caches it in Redis, and returns details
func (rm *RedisManager) fetchAndCacheCompanyDetails(ctx context.Context, isin string) (*models.CompanyDetails, error) {
	if rm.mongoClient == nil {
		return nil, fmt.Errorf("mongo client not initialized in redis manager")
	}

	coll := rm.mongoClient.Client.Database("OdinMasterData").Collection("CompanyMaster")
	
	var doc bson.M
	err := coll.FindOne(ctx, bson.M{"isin": isin}).Decode(&doc)
	if err != nil {
		// If not found in DB, return nil (valid case, company doesn't exist)
		// We could optionally cache a "not found" marker to prevent repeated DB hits for invalid ISINs
		rm.lgr.Warn("company not found in mongodb", zap.String("isin", isin), zap.Error(err))
		return nil, nil 
	}

	// Extract details
	bseCode, _ := doc["bsecode"].(string)
	rm.lgr.Info("company", zap.Any("doc", doc))
	var nseCode string
	// Check for 'code' or 'Code' or 'nsecode' and handle various types
	if v, ok := doc["code"]; ok && v != nil {
		nseCode = fmt.Sprintf("%v", v)
	} else if v, ok := doc["Code"]; ok && v != nil {
		nseCode = fmt.Sprintf("%v", v)
	} else if v, ok := doc["nsecode"]; ok && v != nil {
		nseCode = fmt.Sprintf("%v", v)
	}
	nseCode = strings.TrimSpace(nseCode)

	var mcapValue float64
	if mcap, ok := doc["mcap"]; ok && mcap != nil {
		switch v := mcap.(type) {
		case float64:
			mcapValue = v
		case float32:
			mcapValue = float64(v)
		case int:
			mcapValue = float64(v)
		case int32:
			mcapValue = float64(v)
		case int64:
			mcapValue = float64(v)
		}
	}

	mcapType, _ := doc["mcaptype"].(string)

	nseStatus, _ := doc["NSEStatus"].(string)
	bseStatus, _ := doc["BSEStatus"].(string)

	exchange := ""
	if strings.EqualFold(strings.TrimSpace(nseStatus), "Active") {
		exchange = "NSE"
	} else if strings.EqualFold(strings.TrimSpace(bseStatus), "Active") {
		exchange = "BSE"
	} else {
		// Company is inactive
		return nil, nil
	}

	details := models.CompanyDetails{
		ISIN:     isin,
		BSECode:  bseCode,
		NSECode:  nseCode,
		MCap:     mcapValue,
		MCapType: mcapType,
		Exchange: exchange,
	}

	// Cache in Redis
	data, err := json.Marshal(details)
	if err == nil {
		key := fmt.Sprintf("isin:%s", isin)
		// Use same TTL as bulk load
		if err := rm.client.Set(ctx, key, data, 25*time.Hour).Err(); err != nil {
			rm.lgr.Error("failed to update redis cache", zap.String("isin", isin), zap.Error(err))
		} else {
			rm.lgr.Info("updated redis cache from mongodb", zap.String("isin", isin))
		}
	}

	return &details, nil
}

// LoadCompanyMasterData loads data from MongoDB to Redis
func (rm *RedisManager) LoadCompanyMasterData(ctx context.Context, mongoClient *mongodb.Client) error {
	rm.lgr.Info("starting company master data to redis sync")
	
	coll := mongoClient.Client.Database("OdinMasterData").Collection("CompanyMaster")
	
	// Find all active companies (or all companies if needed for lookups)
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to query company master: %w", err)
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			rm.lgr.Error("failed to decode company doc", zap.Error(err))
			continue
		}

		isin, _ := doc["isin"].(string)
		if isin == "" {
			continue
		}

		// Extract fields based on requirements
		bseCode, _ := doc["bsecode"].(string)
		
		// Derive NSE Code
		// Logic: "nsecode -> derive from code from CompanyMaster note sometimes code is not present so create null"
		var nseCode string
		if v, ok := doc["code"]; ok && v != nil {
			nseCode = fmt.Sprintf("%v", v)
		} else if v, ok := doc["Code"]; ok && v != nil {
			nseCode = fmt.Sprintf("%v", v)
		} else if v, ok := doc["nsecode"]; ok && v != nil {
			nseCode = fmt.Sprintf("%v", v)
		}
		nseCode = strings.TrimSpace(nseCode)

		// Extract MCap
		var mcapValue float64
		if mcap, ok := doc["mcap"]; ok && mcap != nil {
			switch v := mcap.(type) {
			case float64:
				mcapValue = v
			case float32:
				mcapValue = float64(v)
			case int:
				mcapValue = float64(v)
			case int32:
				mcapValue = float64(v)
			case int64:
				mcapValue = float64(v)
			}
		}

		mcapType, _ := doc["mcaptype"].(string)

		// Determine Exchange
		// Logic: "Exchange -> if NSEStatus is active then exchnse will be NSE and if nse is nto hen check in BSEStatus if it is active then exahnge wil be BSE and both are not active then we dont process their news"
		nseStatus, _ := doc["NSEStatus"].(string)
		bseStatus, _ := doc["BSEStatus"].(string)

		exchange := ""
		if strings.EqualFold(strings.TrimSpace(nseStatus), "Active") {
			exchange = "NSE"
		} else if strings.EqualFold(strings.TrimSpace(bseStatus), "Active") {
			exchange = "BSE"
		} else {
			// Skip storing companies that are not active in either? 
			// Requirement says: "both are not active then we dont process their news"
			// So we can arguably skip inserting them into Redis lookup table to save space and implicitly filter them out later.
			// Or we can store with "INACTIVE" and handle logic in watcher.
			// Storing everything is safer, but "we have to only keep which we are using" suggests filtered load.
			// Let's store only active ones as per "so in kafka we have to only keep which we are using"
			continue 
		}

		// if exchange == "NSE" && nseCode == "" {
		// 	rm.lgr.Warn("company active in NSE but nseCode (code) is missing", zap.String("isin", isin))
		// }

		details := models.CompanyDetails{
			ISIN:     isin,
			BSECode:  bseCode,
			NSECode:  nseCode,
			MCap:     mcapValue,
			MCapType: mcapType,
			Exchange: exchange,
		}

		data, err := json.Marshal(details)
		if err != nil {
			continue
		}

		key := fmt.Sprintf("isin:%s", isin)
		if err := rm.client.Set(ctx, key, data, 25*time.Hour).Err(); err != nil { // 25 hours TTL to ensure overlap
			rm.lgr.Error("failed to set redis key", zap.String("isin", isin), zap.Error(err))
		}
		count++
	}

	rm.lgr.Info("finished company master sync", zap.Int("companies_loaded", count))
	return nil
}

// StartScheduler starts the daily sync job
func (rm *RedisManager) StartScheduler(ctx context.Context, mongoClient *mongodb.Client) {
	// Schedule first run immediately? Or wait for 8 AM?
	// Requirement: "we will update redis lookup table evry mornig at 08:00 am"
	// But on startup we probably need data immediately if redis is empty.
	// So let's run once on startup, then schedule.
	
	go func() {
		if err := rm.LoadCompanyMasterData(ctx, mongoClient); err != nil {
			rm.lgr.Error("initial company master load failed", zap.Error(err))
		}
	}()

	go func() {
		for {
			now := time.Now()
			// Calculate next 8:00 AM
			nextRun := time.Date(now.Year(), now.Month(), now.Day(), 8, 0, 0, 0, now.Location())
			if now.After(nextRun) {
				nextRun = nextRun.Add(24 * time.Hour)
			}

			duration := nextRun.Sub(now)
			rm.lgr.Info("scheduling next company master sync", zap.Duration("wait", duration))

			select {
			case <-time.After(duration):
				if err := rm.LoadCompanyMasterData(ctx, mongoClient); err != nil {
					rm.lgr.Error("scheduled company master load failed", zap.Error(err))
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// ResolveCompany implements manthan.CompanyResolver. Returns the NSE/BSE codes
// + active exchange for an ISIN. Hits Redis first; on miss falls back to
// MongoDB CompanyMaster and writes the result into Redis (so subsequent
// trade-execution lookups for the same ISIN are guaranteed to hit cache).
//
// Returns (empty, empty, empty, nil) when the ISIN exists in CompanyMaster
// but neither exchange is Active — caller should treat this as un-tradeable.
// Returns ("","","",nil) when ISIN is genuinely missing in master data.
// Returns ("","","",err) on transport / decode failures.
func (rm *RedisManager) ResolveCompany(ctx context.Context, isin string) (nseCode, bseCode, exchange string, err error) {
	details, err := rm.GetCompanyDetails(ctx, isin)
	if err != nil {
		return "", "", "", err
	}
	if details == nil {
		return "", "", "", nil // missing in master / inactive on both exchanges
	}
	return details.NSECode, details.BSECode, details.Exchange, nil
}
