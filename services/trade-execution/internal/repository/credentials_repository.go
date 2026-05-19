package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/RohitIndira/Algo-Treading/pkg/crypto"
	"github.com/jmoiron/sqlx"
)

type CredentialsRepository interface {
	// StoreIndiraCredentials upserts Indira Securities credentials for a user.
	StoreIndiraCredentials(ctx context.Context, userID, userId, appId, source, bearerToken string) error

	// GetIndiraCredentials retrieves Indira Securities credentials for a user.
	GetIndiraCredentials(ctx context.Context, userID string) (userId, appId, source, bearerToken string, err error)
}

type credentialsRepository struct {
	db            *sqlx.DB
	encryptionKey string
}

func NewCredentialsRepository(db *sqlx.DB, encryptionKey string) CredentialsRepository {
	return &credentialsRepository{
		db:            db,
		encryptionKey: encryptionKey,
	}
}

// StoreIndiraCredentials upserts credentials into broker_accounts.
// This is the canonical credentials table (written by user-config service on strategy create/update).
func (r *credentialsRepository) StoreIndiraCredentials(ctx context.Context, userID, brokerUserID, appId, source, bearerToken string) error {
	encryptedToken, err := crypto.Encrypt(bearerToken, r.encryptionKey)
	if err != nil {
		return err
	}

	query := `
		INSERT INTO broker_accounts (user_id, broker_name, broker_user_id, app_id, source, bearer_token, token_updated_at, updated_at)
		VALUES ($1, 'INDIRA', $2, $3, $4, $5, NOW(), NOW())
		ON CONFLICT (user_id, broker_name)
		DO UPDATE SET
			broker_user_id   = EXCLUDED.broker_user_id,
			app_id           = EXCLUDED.app_id,
			source           = EXCLUDED.source,
			bearer_token     = EXCLUDED.bearer_token,
			token_updated_at = NOW(),
			updated_at       = NOW()
	`
	_, err = r.db.ExecContext(ctx, query, userID, brokerUserID, appId, source, encryptedToken)
	return err
}

// GetIndiraCredentials reads from broker_accounts — the table written by user-config.
// Falls back to user_credentials (legacy table) if no row is found there.
func (r *credentialsRepository) GetIndiraCredentials(ctx context.Context, userID string) (string, string, string, string, error) {
	// Primary: broker_accounts (written by user-config on strategy create/update)
	brokerUserID, appId, source, encryptedToken, err := r.fromBrokerAccounts(ctx, userID)
	if err == nil {
		token, decErr := crypto.Decrypt(encryptedToken, r.encryptionKey)
		if decErr != nil {
			return "", "", "", "", decErr
		}
		return brokerUserID, appId, source, token, nil
	}
	if !errors.Is(err, ErrCredentialsNotFound) {
		return "", "", "", "", err
	}

	// Fallback: user_credentials (legacy table, kept for backward compat)
	return r.fromUserCredentials(ctx, userID)
}

func (r *credentialsRepository) fromBrokerAccounts(ctx context.Context, userID string) (brokerUserID, appId, source, bearerToken string, err error) {
	query := `
		SELECT broker_user_id, app_id, source, bearer_token
		FROM broker_accounts
		WHERE user_id = $1 AND broker_name = 'INDIRA'
	`
	row := r.db.QueryRowxContext(ctx, query, userID)
	err = row.Scan(&brokerUserID, &appId, &source, &bearerToken)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", "", "", ErrCredentialsNotFound
		}
		return "", "", "", "", err
	}
	return brokerUserID, appId, source, bearerToken, nil
}

func (r *credentialsRepository) fromUserCredentials(ctx context.Context, userID string) (string, string, string, string, error) {
	query := `
		SELECT indira_user_id, indira_app_id, indira_source, indira_bearer_token
		FROM user_credentials
		WHERE user_id = $1
	`
	var brokerUserID, appId, source, encryptedToken string
	row := r.db.QueryRowxContext(ctx, query, userID)
	err := row.Scan(&brokerUserID, &appId, &source, &encryptedToken)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", "", "", ErrCredentialsNotFound
		}
		return "", "", "", "", err
	}
	token, err := crypto.Decrypt(encryptedToken, r.encryptionKey)
	if err != nil {
		return "", "", "", "", err
	}
	return brokerUserID, appId, source, token, nil
}

var ErrCredentialsNotFound = errors.New("credentials not found")
