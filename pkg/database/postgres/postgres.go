package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// Config holds PostgreSQL configuration
type Config struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Client wraps sql.DB with additional functionality
type Client struct {
	*sql.DB
	config Config
}

// New creates a new PostgreSQL client
func New(config Config) (*Client, error) {
	// Set default values
	if config.Port == 0 {
		config.Port = 5432
	}
	if config.SSLMode == "" {
		config.SSLMode = "disable"
	}
	if config.MaxOpenConns == 0 {
		config.MaxOpenConns = 25
	}
	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = 5
	}
	if config.ConnMaxLifetime == 0 {
		config.ConnMaxLifetime = 5 * time.Minute
	}
	if config.ConnMaxIdleTime == 0 {
		config.ConnMaxIdleTime = 5 * time.Minute
	}

	// Build connection string
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host,
		config.Port,
		config.User,
		config.Password,
		config.Database,
		config.SSLMode,
	)

	// Open database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL connection: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	return &Client{
		DB:     db,
		config: config,
	}, nil
}

// Close closes the PostgreSQL connection
func (c *Client) Close() error {
	return c.DB.Close()
}

// HealthCheck performs a health check on the PostgreSQL connection
func (c *Client) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	return c.PingContext(ctx)
}

// BeginTx starts a transaction with the given options
func (c *Client) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return c.DB.BeginTx(ctx, opts)
}

// ExecuteInTransaction executes a function within a transaction
func (c *Client) ExecuteInTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := c.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("transaction error: %v, rollback error: %w", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// QueryOne executes a query that returns a single row
func (c *Client) QueryOne(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.QueryRowContext(ctx, query, args...)
}

// QueryMany executes a query that returns multiple rows
func (c *Client) QueryMany(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.QueryContext(ctx, query, args...)
}

// Execute executes a query that doesn't return rows (INSERT, UPDATE, DELETE)
func (c *Client) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.ExecContext(ctx, query, args...)
}

// GetDatabaseName returns the configured database name
func (c *Client) GetDatabaseName() string {
	return c.config.Database
}

// GetStats returns database statistics
func (c *Client) GetStats() sql.DBStats {
	return c.Stats()
}
