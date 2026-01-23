package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// UserCredentials represents user's Odin API credentials as used by the
// trade-execution service. NULL-able DB fields are normalised to empty
// strings so that we don't fail scanning when some users don't yet have
// encrypted passwords or TOTP configured.
type UserCredentials struct {
	UserID            string
	APIKEY            string // Odin user ID for login
	APIURL            string // Odin base URL for this user's API (optional, may be empty)
	PasswordEncrypted string // Encrypted password (may be empty in dev)
	TOTPSecret        string // TOTP secret for 2FA (may be empty in dev)
	IsActive          bool
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
	// Use an internal struct with sql.NullString for NULL-able columns so
	// we don't get scan errors when password_encrypted or totp_secret are
	// NULL (which can be valid for some onboarding flows).
	type dbUserCredentials struct {
		UserID            string         `db:"user_id"`
		APIKEY            string         `db:"api_key"`
		APIURL            sql.NullString `db:"api_url"`
		PasswordEncrypted sql.NullString `db:"password_encrypted"`
		TOTPSecret        sql.NullString `db:"totp_secret"`
		IsActive          bool           `db:"is_active"`
	}

	var row dbUserCredentials
	query := `
		SELECT user_id, api_key, api_url, password_encrypted, totp_secret, is_active
		FROM user_credentials 
		WHERE user_id = $1 AND is_active = true
	`

	err := r.db.GetContext(ctx, &row, query, userID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("credentials not found for user: %s", userID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user credentials: %w", err)
	}

	creds := &UserCredentials{
		UserID:   row.UserID,
		APIKEY:   row.APIKEY,
		IsActive: row.IsActive,
	}
	if row.APIURL.Valid {
		creds.APIURL = row.APIURL.String
	}
	if row.PasswordEncrypted.Valid {
		creds.PasswordEncrypted = row.PasswordEncrypted.String
	}
	if row.TOTPSecret.Valid {
		creds.TOTPSecret = row.TOTPSecret.String
	}

	return creds, nil
}
