// In services/trade-execution/internal/repository/credentials_repository.go

package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
)

type CredentialsRepository interface {
	// StoreIndiraCredentials stores Indira Securities credentials for a user
	StoreIndiraCredentials(ctx context.Context, userID, userId, appId, source, bearerToken string) error

	// GetIndiraCredentials retrieves Indira Securities credentials for a user
	GetIndiraCredentials(ctx context.Context, userID string) (userId, appId, source, bearerToken string, err error)
}

type credentialsRepository struct {
	db *sqlx.DB
}

func NewCredentialsRepository(db *sqlx.DB) CredentialsRepository {
	return &credentialsRepository{db: db}
}

func (r *credentialsRepository) StoreIndiraCredentials(ctx context.Context, userID, userId, appId, source, bearerToken string) error {
	query := `
        INSERT INTO user_credentials (user_id, indira_user_id, indira_app_id, indira_source, indira_bearer_token, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (user_id) 
        DO UPDATE SET 
            indira_user_id = EXCLUDED.indira_user_id,
            indira_app_id = EXCLUDED.indira_app_id,
            indira_source = EXCLUDED.indira_source,
            indira_bearer_token = EXCLUDED.indira_bearer_token,
            updated_at = NOW()
        RETURNING id
    `

	_, err := r.db.ExecContext(ctx, query,
		userID, userId, appId, source, bearerToken)
	return err
}

func (r *credentialsRepository) GetIndiraCredentials(ctx context.Context, userID string) (string, string, string, string, error) {
	var userId, appId, source, bearerToken string

	query := `
        SELECT indira_user_id, indira_app_id, indira_source, indira_bearer_token
        FROM user_credentials
        WHERE user_id = $1
    `

	row := r.db.QueryRowxContext(ctx, query, userID)
	err := row.Scan(&userId, &appId, &source, &bearerToken)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", "", "", ErrCredentialsNotFound
		}
		return "", "", "", "", err
	}

	return userId, appId, source, bearerToken, nil
}

var ErrCredentialsNotFound = errors.New("credentials not found")
