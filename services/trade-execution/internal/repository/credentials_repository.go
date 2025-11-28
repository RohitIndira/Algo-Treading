package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// UserCredentials represents user's Odin API credentials from user_credentials table
type UserCredentials struct {
	UserID            string `db:"user_id"`
	APIKEY            string `db:"api_key"`            // Odin user ID for login
	PasswordEncrypted string `db:"password_encrypted"` // Encrypted password
	TOTPSecret        string `db:"totp_secret"`        // TOTP secret for 2FA
	IsActive          bool   `db:"is_active"`
}

// CredentialsRepository defines database operations for user credentials
type CredentialsRepository interface {
	GetUserCredentials(ctx context.Context, userID string) (*UserCredentials, error)
}

type credentialsRepository struct {
	db *sqlx.DB
}

// NewCredentialsRepository creates a new credentials repository
func NewCredentialsRepository(db *sqlx.DB) CredentialsRepository {
	return &credentialsRepository{db: db}
}

// GetUserCredentials retrieves Odin API credentials for a user
func (r *credentialsRepository) GetUserCredentials(ctx context.Context, userID string) (*UserCredentials, error) {
	var creds UserCredentials
	query := `
		SELECT user_id, api_key, password_encrypted, totp_secret, is_active
		FROM user_credentials 
		WHERE user_id = $1 AND is_active = true
	`

	err := r.db.GetContext(ctx, &creds, query, userID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("credentials not found for user: %s", userID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user credentials: %w", err)
	}

	return &creds, nil
}
