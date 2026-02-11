package index

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
)

// QueryEngine handles Elasticsearch queries
type QueryEngine struct {
	client    *elasticsearch.Client
	indexName string
	logger    *zap.Logger
	timeout   time.Duration
}

// NewQueryEngine creates a new query engine
func NewQueryEngine(client *elasticsearch.Client, indexName string, timeout time.Duration, logger *zap.Logger) *QueryEngine {
	return &QueryEngine{
		client:    client,
		indexName: indexName,
		logger:    logger,
		timeout:   timeout,
	}
}

// FindMatchingStrategies finds strategies that might match the event
func (q *QueryEngine) FindMatchingStrategies(ctx context.Context, event *models.MarketEvent) ([]*models.ElasticsearchStrategy, error) {
	startTime := time.Now()

	query := q.buildQuery(event)

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("failed to encode query: %w", err)
	}

	// Add timeout to context
	queryCtx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	res, err := q.client.Search(
		q.client.Search.WithContext(queryCtx),
		q.client.Search.WithIndex(q.indexName),
		q.client.Search.WithBody(&buf),
		q.client.Search.WithSize(1000), // Maximum candidates
		q.client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch search error: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch search failed: %s", res.Status())
	}

	var result struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source models.ElasticsearchStrategy `json:"_source"`
				Score  float64                      `json:"_score"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	strategies := make([]*models.ElasticsearchStrategy, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		strategies = append(strategies, &hit.Source)
	}

	queryDuration := time.Since(startTime)
	q.logger.Debug("Elasticsearch query completed",
		zap.Int("candidates_found", len(strategies)),
		zap.Duration("duration", queryDuration),
		zap.String("event_id", event.EventID))

	return strategies, nil
}

// buildQuery builds the Elasticsearch query for matching strategies
func (q *QueryEngine) buildQuery(event *models.MarketEvent) map[string]interface{} {
	sentiment := event.Analysis.GetSentimentValue()

	// Build bool query with multiple conditions
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					// Strategy must be active
					map[string]interface{}{
						"term": map[string]interface{}{
							"active": true,
						},
					},
				},
				"filter": []interface{}{
					// Impact score: strategy's min threshold should be <= event's impact score
					map[string]interface{}{
						"range": map[string]interface{}{
							"impact_score_min": map[string]interface{}{
								"lte": event.Analysis.ImpactScore,
							},
						},
					},
				},
				"should":               []interface{}{},
				"minimum_should_match": 0,
			},
		},
	}

	boolQuery := query["query"].(map[string]interface{})["bool"].(map[string]interface{})
	shouldClauses := []interface{}{}

	// Add sentiment match (optional but boosts score)
	shouldClauses = append(shouldClauses, map[string]interface{}{
		"terms": map[string]interface{}{
			"sentiments": []string{sentiment},
			"boost":      2.0,
		},
	})

	// Add category match (optional but boosts score)
	if event.NewsData.Category != "" {
		shouldClauses = append(shouldClauses, map[string]interface{}{
			"terms": map[string]interface{}{
				"categories": []string{event.NewsData.Category},
				"boost":      2.0,
			},
		})
	}

	// Add stock match (optional but boosts score)
	shouldClauses = append(shouldClauses, map[string]interface{}{
		"terms": map[string]interface{}{
			"stocks": []int64{event.StockData.StockCode},
			"boost":  3.0,
		},
	})

	// Add exchange match (optional but boosts score)
	if event.StockData.Exchange != "" {
		shouldClauses = append(shouldClauses, map[string]interface{}{
			"term": map[string]interface{}{
				"exchange": map[string]interface{}{
					"value": event.StockData.Exchange,
					"boost": 1.5,
				},
			},
		})
	}

	// Add price range filter (if price is available)
	if event.MarketData.LastTradedPrice > 0 {
		// Find strategies where price is within range OR range is not set (0,0)
		shouldClauses = append(shouldClauses, map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []interface{}{
					// Price within range
					map[string]interface{}{
						"bool": map[string]interface{}{
							"must": []interface{}{
								map[string]interface{}{
									"range": map[string]interface{}{
										"price_min": map[string]interface{}{
											"lte": event.MarketData.LastTradedPrice,
										},
									},
								},
								map[string]interface{}{
									"range": map[string]interface{}{
										"price_max": map[string]interface{}{
											"gte": event.MarketData.LastTradedPrice,
										},
									},
								},
							},
						},
					},
					// No price filter (both min and max are 0)
					map[string]interface{}{
						"bool": map[string]interface{}{
							"must": []interface{}{
								map[string]interface{}{
									"term": map[string]interface{}{
										"price_min": 0,
									},
								},
								map[string]interface{}{
									"term": map[string]interface{}{
										"price_max": 0,
									},
								},
							},
						},
					},
				},
			},
		})
	}

	// Add volume filter (if volume is available)
	if event.MarketData.PriceMap.Volume > 0 {
		shouldClauses = append(shouldClauses, map[string]interface{}{
			"range": map[string]interface{}{
				"volume_min": map[string]interface{}{
					"lte": event.MarketData.PriceMap.Volume,
				},
			},
		})
	}

	// Add percent change filter (if available)
	absPctChange := math.Abs(event.MarketData.PctChange)
	if absPctChange > 0 {
		shouldClauses = append(shouldClauses, map[string]interface{}{
			"range": map[string]interface{}{
				"pct_change_min": map[string]interface{}{
					"lte": absPctChange,
				},
			},
		})
	}

	boolQuery["should"] = shouldClauses

	// Changed from 1 to 0: Allow strategies with empty conditions (catch-all strategies)
	// to be selected as candidates. The evaluator will handle precise matching.
	// This fixes the issue where strategies with empty stock_codes, sentiments, etc.
	// were not being selected as candidates even though they should match all events.
	boolQuery["minimum_should_match"] = 0

	return query
}

// CountActiveStrategies counts active strategies
func (q *QueryEngine) CountActiveStrategies(ctx context.Context) (int, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"active": true,
			},
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return 0, fmt.Errorf("failed to encode query: %w", err)
	}

	res, err := q.client.Count(
		q.client.Count.WithContext(ctx),
		q.client.Count.WithIndex(q.indexName),
		q.client.Count.WithBody(&buf),
	)
	if err != nil {
		return 0, fmt.Errorf("elasticsearch count error: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("elasticsearch count failed: %s", res.Status())
	}

	var result struct {
		Count int `json:"count"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Count, nil
}

// CountActiveUsers counts unique users with active strategies
func (q *QueryEngine) CountActiveUsers(ctx context.Context) (int, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"active": true,
			},
		},
		"aggs": map[string]interface{}{
			"unique_users": map[string]interface{}{
				"cardinality": map[string]interface{}{
					"field": "user_id",
				},
			},
		},
		"size": 0,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return 0, fmt.Errorf("failed to encode query: %w", err)
	}

	res, err := q.client.Search(
		q.client.Search.WithContext(ctx),
		q.client.Search.WithIndex(q.indexName),
		q.client.Search.WithBody(&buf),
	)
	if err != nil {
		return 0, fmt.Errorf("elasticsearch search error: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("elasticsearch search failed: %s", res.Status())
	}

	var result struct {
		Aggregations struct {
			UniqueUsers struct {
				Value int `json:"value"`
			} `json:"unique_users"`
		} `json:"aggregations"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Aggregations.UniqueUsers.Value, nil
}

// ListUsersWithActiveStrategy returns the list of user_ids that have an
// active strategy with the given strategy name.
func (q *QueryEngine) ListUsersWithActiveStrategy(ctx context.Context, strategyID string) ([]string, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"term": map[string]interface{}{
							"active": true,
						},
					},
					map[string]interface{}{
						// strategy_name is mapped as a text field.
						"match": map[string]interface{}{
							"strategy_name": strategyID,
						},
					},
				},
			},
		},
		"aggs": map[string]interface{}{
			"users": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "user_id",
					"size":  10000,
				},
			},
		},
		"size": 0,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("failed to encode query: %w", err)
	}

	res, err := q.client.Search(
		q.client.Search.WithContext(ctx),
		q.client.Search.WithIndex(q.indexName),
		q.client.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch search error: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch search failed: %s", res.Status())
	}

	var result struct {
		Aggregations struct {
			Users struct {
				Buckets []struct {
					KeyAsString string  `json:"key_as_string"`
					Key         string  `json:"key"`
					DocCount    float64 `json:"doc_count"`
				} `json:"buckets"`
			} `json:"users"`
		} `json:"aggregations"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	userIDs := make([]string, 0, len(result.Aggregations.Users.Buckets))
	for _, b := range result.Aggregations.Users.Buckets {
		if b.Key != "" {
			userIDs = append(userIDs, b.Key)
		}
	}

	q.logger.Info("Refreshed users for strategy",
		zap.String("strategy_id", strategyID),
		zap.Int("user_count", len(userIDs)))

	return userIDs, nil
}
