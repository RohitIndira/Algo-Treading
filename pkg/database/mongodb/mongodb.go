package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Config holds MongoDB configuration
type Config struct {
	URI            string
	Database       string
	ConnectTimeout time.Duration
	MaxPoolSize    uint64
	MinPoolSize    uint64
}

// Client wraps mongo.Client with additional functionality
type Client struct {
	*mongo.Client
	database *mongo.Database
	config   Config
}

// New creates a new MongoDB client
func New(ctx context.Context, config Config) (*Client, error) {
	// Set default values
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 10 * time.Second
	}
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 100
	}
	if config.MinPoolSize == 0 {
		config.MinPoolSize = 10
	}

	// Create client options
	clientOpts := options.Client().
		ApplyURI(config.URI).
		SetMaxPoolSize(config.MaxPoolSize).
		SetMinPoolSize(config.MinPoolSize).
		SetConnectTimeout(config.ConnectTimeout).
		SetServerSelectionTimeout(config.ConnectTimeout)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &Client{
		Client:   client,
		database: client.Database(config.Database),
		config:   config,
	}, nil
}

// Database returns the configured database
func (c *Client) Database() *mongo.Database {
	return c.database
}

// Collection returns a collection from the configured database
func (c *Client) Collection(name string) *mongo.Collection {
	return c.database.Collection(name)
}

// Close closes the MongoDB connection
func (c *Client) Close(ctx context.Context) error {
	return c.Disconnect(ctx)
}

// Ping verifies the connection to MongoDB
func (c *Client) Ping(ctx context.Context) error {
	return c.Client.Ping(ctx, readpref.Primary())
}

// HealthCheck performs a health check on the MongoDB connection
func (c *Client) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return c.Ping(ctx)
}

// WatchCollection creates a change stream for a collection
func (c *Client) WatchCollection(ctx context.Context, collectionName string, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	collection := c.Collection(collectionName)
	return collection.Watch(ctx, pipeline, opts...)
}

// GetDatabaseName returns the configured database name
func (c *Client) GetDatabaseName() string {
	return c.config.Database
}
