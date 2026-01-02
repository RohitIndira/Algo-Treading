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

// buildQuery builds the Elasticsearch query for matching market depth strategies
func (q *QueryEngine) buildQuery(event *models.MarketEvent) map[string]interface{} {
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
				"filter":               []interface{}{},
				"should":               []interface{}{},
				"minimum_should_match": 0,
			},
		},
	}

	boolQuery := query["query"].(map[string]interface{})["bool"].(map[string]interface{})
	filterClauses := []interface{}(boolQuery["filter"].([]interface{}))
	shouldClauses := []interface{}{}

	// Add stock match (higher priority)
	shouldClauses = append(shouldClauses, map[string]interface{}{
		"terms": map[string]interface{}{
			"stocks": []int64{event.StockData.StockCode},
			"boost":  3.0,
		},
	})

	// Add exchange match (optional)
	if event.StockData.Exchange != "" {
		shouldClauses = append(shouldClauses, map[string]interface{}{
			"terms": map[string]interface{}{
				"exchanges": []string{event.StockData.Exchange},
				"boost":     1.5,
			},
		})
	}

	// Add price range filter (if price is available and strategy has price constraints)
	if event.MarketData.LastTradedPrice > 0 {
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

	// Add market depth filters (most important for market depth trading)
	// Strategies with market depth conditions will match if their conditions are compatible
	shouldClauses = append(shouldClauses, map[string]interface{}{
		"bool": map[string]interface{}{
			"should": []interface{}{
				// Strategies with no min bid/ask quantity requirement
				map[string]interface{}{
					"term": map[string]interface{}{
						"min_bid_qty": 0,
					},
				},
				// Strategies where event's bid quantity meets minimum
				map[string]interface{}{
					"range": map[string]interface{}{
						"min_bid_qty": map[string]interface{}{
							"lte": int64(0),
						},
					},
				},
			},
		},
	})

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

	boolQuery["filter"] = filterClauses
	boolQuery["should"] = shouldClauses
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
