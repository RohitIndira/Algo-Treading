package index

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.uber.org/zap"
)

// Indexer manages Elasticsearch indexing operations
type Indexer struct {
	client    *elasticsearch.Client
	indexName string
	logger    *zap.Logger
}

// NewIndexer creates a new Elasticsearch indexer
func NewIndexer(urls []string, username, password, indexName string, logger *zap.Logger) (*Indexer, error) {
	cfg := elasticsearch.Config{
		Addresses:     urls,
		Username:      username,
		Password:      password,
		RetryOnStatus: []int{502, 503, 504, 429},
		MaxRetries:    3,
		RetryBackoff: func(i int) time.Duration {
			return time.Duration(i) * time.Second
		},
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	indexer := &Indexer{
		client:    client,
		indexName: indexName,
		logger:    logger,
	}

	// Ping to verify connection
	if err := indexer.ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping elasticsearch: %w", err)
	}

	// Ensure index exists
	if err := indexer.ensureIndexExists(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ensure index exists: %w", err)
	}

	logger.Info("Elasticsearch indexer initialized",
		zap.String("index", indexName),
		zap.Strings("urls", urls))

	return indexer, nil
}

// ping verifies Elasticsearch connection
func (i *Indexer) ping(ctx context.Context) error {
	res, err := i.client.Ping(
		i.client.Ping.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch ping failed: %s", res.Status())
	}

	return nil
}

// ensureIndexExists creates the index if it doesn't exist
func (i *Indexer) ensureIndexExists(ctx context.Context) error {
	// Check if index exists
	res, err := i.client.Indices.Exists(
		[]string{i.indexName},
		i.client.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// Index already exists
	if res.StatusCode == 200 {
		i.logger.Info("Elasticsearch index already exists", zap.String("index", i.indexName))
		return nil
	}

	// Create index with mapping
	mapping := i.getIndexMapping()
	res, err = i.client.Indices.Create(
		i.indexName,
		i.client.Indices.Create.WithContext(ctx),
		i.client.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to create index: %s", res.Status())
	}

	i.logger.Info("Elasticsearch index created", zap.String("index", i.indexName))
	return nil
}

// getIndexMapping returns the index mapping configuration
func (i *Indexer) getIndexMapping() string {
	return `{
		"settings": {
			"number_of_shards": 3,
			"number_of_replicas": 2,
			"refresh_interval": "5s",
			"max_result_window": 10000
		},
		"mappings": {
			"properties": {
				"strategy_id": {"type": "keyword"},
				"user_id": {"type": "keyword"},
				"strategy_name": {"type": "text"},
				"active": {"type": "boolean"},
				"impact_score_min": {"type": "integer"},
				"sentiments": {"type": "keyword"},
				"categories": {"type": "keyword"},
				"stocks": {"type": "long"},
				"price_min": {"type": "double"},
				"price_max": {"type": "double"},
				"volume_min": {"type": "long"},
				"pct_change_min": {"type": "double"},
				"exchange": {"type": "keyword"},
				"max_daily_trades": {"type": "integer"},
				"max_loss_per_day": {"type": "double"},
				"updated_at": {"type": "long"}
			}
		}
	}`
}

// IndexStrategy indexes a strategy
func (i *Indexer) IndexStrategy(ctx context.Context, strategy *models.Strategy) error {
	esStrategy := strategy.ToElasticsearchStrategy()

	data, err := json.Marshal(esStrategy)
	if err != nil {
		return fmt.Errorf("failed to marshal strategy: %w", err)
	}

	req := esapi.IndexRequest{
		Index:      i.indexName,
		DocumentID: strategy.StrategyID,
		Body:       bytes.NewReader(data),
		Refresh:    "true",
	}

	res, err := req.Do(ctx, i.client)
	if err != nil {
		return fmt.Errorf("failed to index strategy: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch indexing error: %s", res.Status())
	}

	i.logger.Debug("Strategy indexed",
		zap.String("strategy_id", strategy.StrategyID),
		zap.String("user_id", strategy.UserID))

	return nil
}

// DeleteStrategy removes a strategy from the index
func (i *Indexer) DeleteStrategy(ctx context.Context, strategyID string) error {
	req := esapi.DeleteRequest{
		Index:      i.indexName,
		DocumentID: strategyID,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, i.client)
	if err != nil {
		return fmt.Errorf("failed to delete strategy: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != 404 {
		return fmt.Errorf("elasticsearch delete error: %s", res.Status())
	}

	i.logger.Debug("Strategy deleted", zap.String("strategy_id", strategyID))
	return nil
}

// BulkIndexStrategies indexes multiple strategies in bulk
func (i *Indexer) BulkIndexStrategies(ctx context.Context, strategies []*models.Strategy) error {
	if len(strategies) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, strategy := range strategies {
		esStrategy := strategy.ToElasticsearchStrategy()

		// Action metadata
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": i.indexName,
				"_id":    strategy.StrategyID,
			},
		}

		metaData, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal meta: %w", err)
		}

		docData, err := json.Marshal(esStrategy)
		if err != nil {
			return fmt.Errorf("failed to marshal strategy: %w", err)
		}

		buf.Write(metaData)
		buf.WriteByte('\n')
		buf.Write(docData)
		buf.WriteByte('\n')
	}

	res, err := i.client.Bulk(
		bytes.NewReader(buf.Bytes()),
		i.client.Bulk.WithContext(ctx),
		i.client.Bulk.WithRefresh("true"),
	)
	if err != nil {
		return fmt.Errorf("failed to bulk index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch bulk indexing error: %s", res.Status())
	}

	// Check for errors in bulk response
	var bulkResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		return fmt.Errorf("failed to decode bulk response: %w", err)
	}

	if bulkResp["errors"].(bool) {
		i.logger.Warn("Bulk indexing had errors", zap.Any("response", bulkResp))
	}

	i.logger.Info("Strategies bulk indexed", zap.Int("count", len(strategies)))
	return nil
}

// GetStrategy retrieves a strategy by ID
func (i *Indexer) GetStrategy(ctx context.Context, strategyID string) (*models.ElasticsearchStrategy, error) {
	req := esapi.GetRequest{
		Index:      i.indexName,
		DocumentID: strategyID,
	}

	res, err := req.Do(ctx, i.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, models.ErrCacheMiss
	}

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch get error: %s", res.Status())
	}

	var result struct {
		Source models.ElasticsearchStrategy `json:"_source"`
	}

	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result.Source, nil
}

// RefreshIndex refreshes the index
func (i *Indexer) RefreshIndex(ctx context.Context) error {
	res, err := i.client.Indices.Refresh(
		i.client.Indices.Refresh.WithIndex(i.indexName),
		i.client.Indices.Refresh.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to refresh index: %s", res.Status())
	}

	return nil
}

// Close closes the indexer (no-op for elasticsearch client)
func (i *Indexer) Close() error {
	i.logger.Info("Elasticsearch indexer closed")
	return nil
}
