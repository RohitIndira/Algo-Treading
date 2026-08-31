package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// SessionTTL is how long one elevation lasts. Deliberately short: an admin
// token left in a browser tab overnight must be worthless by morning.
const SessionTTL = 30 * time.Minute

// Store is the persistence layer over trading_db's admin_* tables
// (migration 019). It owns NO policy — Service does.
type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

var (
	// ErrNotAdmin — user is not on the allow-list or is deactivated.
	ErrNotAdmin = errors.New("not an active admin")
	// ErrSessionInvalid — token unknown, expired, revoked, or admin deactivated.
	ErrSessionInvalid = errors.New("admin session invalid")
)

// IsActiveAdmin reports whether userID is on the allow-list with active=true.
func (s *Store) IsActiveAdmin(ctx context.Context, userID string) (bool, error) {
	var active bool
	err := s.db.QueryRowContext(ctx,
		`SELECT active FROM admin_users WHERE user_id = $1`, userID).Scan(&active)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("admin lookup: %w", err)
	}
	return active, nil
}

// CreateSession inserts a session row for an (already-authorized) admin and
// returns its id. tokenHash is hex(sha256(raw)) — never the raw token.
func (s *Store) CreateSession(ctx context.Context, adminID, tokenHash, ip string) (int64, time.Time, error) {
	expires := time.Now().Add(SessionTTL)
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO admin_sessions (admin_id, token_hash, ip, expires_at)
		VALUES ($1, $2, $3, $4) RETURNING id`,
		adminID, tokenHash, ip, expires).Scan(&id)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("create admin session: %w", err)
	}
	return id, expires, nil
}

// Session is the validated identity attached to admin requests.
type Session struct {
	ID        int64
	AdminID   string
	ExpiresAt time.Time
}

// LookupSession validates a presented token hash end-to-end: session exists,
// not expired, not revoked, AND the admin is still active (deactivating an
// admin kills their live sessions on the next request, not the next login).
func (s *Store) LookupSession(ctx context.Context, tokenHash string) (*Session, error) {
	var sess Session
	err := s.db.QueryRowContext(ctx, `
		SELECT s.id, s.admin_id, s.expires_at
		  FROM admin_sessions s
		  JOIN admin_users u ON u.user_id = s.admin_id
		 WHERE s.token_hash = $1
		   AND s.revoked_at IS NULL
		   AND s.expires_at > now()
		   AND u.active = true`,
		tokenHash).Scan(&sess.ID, &sess.AdminID, &sess.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrSessionInvalid
	}
	if err != nil {
		return nil, fmt.Errorf("session lookup: %w", err)
	}
	return &sess, nil
}

// RevokeSession ends one session (logout). Idempotent.
func (s *Store) RevokeSession(ctx context.Context, sessionID int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE admin_sessions SET revoked_at = now()
		  WHERE id = $1 AND revoked_at IS NULL`, sessionID)
	return err
}

// RevokeAllForAdmin ends every live session for an admin (kill switch).
func (s *Store) RevokeAllForAdmin(ctx context.Context, adminID string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE admin_sessions SET revoked_at = now()
		  WHERE admin_id = $1 AND revoked_at IS NULL`, adminID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// AuditEntry is one row of the append-only trail. Written for every admin
// action INCLUDING denials — the misuse signal is in the failures.
type AuditEntry struct {
	AdminID    string
	Action     string
	Tier       string
	TargetUser string
	TargetRef  string
	Params     any // JSON-marshalled; nil → NULL
	Result     string
	Detail     string
	SelfAction bool
	IP         string
	SessionID  int64 // 0 → NULL (pre-session events like ELEVATE_DENIED)
}

// Audit appends one entry. Failures are returned, never swallowed — callers
// on mutating paths must treat an audit failure as a request failure.
func (s *Store) Audit(ctx context.Context, e AuditEntry) error {
	var params any
	if e.Params != nil {
		b, err := json.Marshal(e.Params)
		if err != nil {
			return fmt.Errorf("audit params marshal: %w", err)
		}
		params = string(b)
	}
	var sessionID any
	if e.SessionID > 0 {
		sessionID = e.SessionID
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO admin_audit
			(admin_id, action, tier, target_user, target_ref, params,
			 result, detail, self_action, ip, session_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		e.AdminID, e.Action, e.Tier, e.TargetUser, e.TargetRef, params,
		e.Result, e.Detail, e.SelfAction, e.IP, sessionID)
	if err != nil {
		return fmt.Errorf("audit insert: %w", err)
	}
	return nil
}

// AuditFilter narrows ListAudit. Zero values mean "no filter".
type AuditFilter struct {
	AdminID    string
	TargetUser string
	Since      time.Time
	Limit      int
}

// AuditRow is a read-model row for the audit viewer.
type AuditRow struct {
	ID         int64           `json:"id"`
	AdminID    string          `json:"admin_id"`
	Action     string          `json:"action"`
	Tier       string          `json:"tier"`
	TargetUser string          `json:"target_user,omitempty"`
	TargetRef  string          `json:"target_ref,omitempty"`
	Params     json.RawMessage `json:"params,omitempty"`
	Result     string          `json:"result"`
	Detail     string          `json:"detail,omitempty"`
	SelfAction bool            `json:"self_action"`
	IP         string          `json:"ip,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

// ListAudit returns newest-first audit rows for the viewer endpoint.
func (s *Store) ListAudit(ctx context.Context, f AuditFilter) ([]AuditRow, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, admin_id, action, tier, target_user, target_ref,
		       COALESCE(params, 'null'::jsonb), result, detail,
		       self_action, ip, created_at
		  FROM admin_audit
		 WHERE ($1 = '' OR admin_id    = $1)
		   AND ($2 = '' OR target_user = $2)
		   AND ($3::timestamptz IS NULL OR created_at >= $3)
		 ORDER BY id DESC
		 LIMIT $4`,
		f.AdminID, f.TargetUser, nullableTime(f.Since), f.Limit)
	if err != nil {
		return nil, fmt.Errorf("audit list: %w", err)
	}
	defer rows.Close()
	var out []AuditRow
	for rows.Next() {
		var r AuditRow
		if err := rows.Scan(&r.ID, &r.AdminID, &r.Action, &r.Tier,
			&r.TargetUser, &r.TargetRef, &r.Params, &r.Result, &r.Detail,
			&r.SelfAction, &r.IP, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
