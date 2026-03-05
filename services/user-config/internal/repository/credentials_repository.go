package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/pkg/crypto"
	"github.com/jmoiron/sqlx"
)

// CredentialsRepository saves/fetches Indira broker credentials in the
// trade-execution database (trading_execution.user_credentials).
type CredentialsRepository interface {
	StoreIndiraCredentials(ctx context.Context, userID, indiraUserID, appID, source, bearerToken string) error
}

type credentialsRepository struct {
	db            *sqlx.DB
	encryptionKey string
}

// NewCredentialsRepository creates a new CredentialsRepository backed by db.
// db must be a connection to the trading_execution database.
func NewCredentialsRepository(db *sqlx.DB, encryptionKey string) CredentialsRepository {
	return &credentialsRepository{
		db:            db,
		encryptionKey: encryptionKey,
	}
}

// StoreIndiraCredentials upserts Indira Securities credentials for a user.
func (r *credentialsRepository) StoreIndiraCredentials(ctx context.Context, userID, indiraUserID, appID, source, bearerToken string) error {
	if userID == "" || bearerToken == "" {
		return fmt.Errorf("userID and bearerToken are required")
	}

	query := `
        INSERT INTO user_credentials (user_id, indira_user_id, indira_app_id, indira_source, indira_bearer_token, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET
            indira_user_id      = EXCLUDED.indira_user_id,
            indira_app_id       = EXCLUDED.indira_app_id,
            indira_source       = EXCLUDED.indira_source,
            indira_bearer_token = EXCLUDED.indira_bearer_token,
            updated_at          = NOW()
    `

	encryptedToken, err := crypto.Encrypt(bearerToken, r.encryptionKey)
	if err != nil {
		return fmt.Errorf("encryption error: %w", err)
	}

	_, err = r.db.ExecContext(ctx, query, userID, indiraUserID, appID, source, encryptedToken)
	if err != nil {
		return fmt.Errorf("failed to store credentials for user %s: %w", userID, err)
	}
	return nil
}

// ErrCredentialsNotFound is returned when no credentials exist for a user.
var ErrCredentialsNotFound = errors.New("credentials not found")

// noopCredentialsRepository is used when no execution DB is configured.
type noopCredentialsRepository struct{}

func NewNoopCredentialsRepository() CredentialsRepository {
	return &noopCredentialsRepository{}
}

func (r *noopCredentialsRepository) StoreIndiraCredentials(_ context.Context, _, _, _, _, _ string) error {
	_ = sql.ErrNoRows // suppress import warning
	return nil        // silently succeed — no execution DB configured
}
