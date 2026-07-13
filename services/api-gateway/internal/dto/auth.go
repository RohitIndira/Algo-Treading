package dto

// UpdateCredentialsRequest is the JSON body of POST /api/v1/auth/credentials.
//
// Posted by the frontend after a successful SSO login (and on every JWT
// refresh — e.g. when SSO issues a fresh token after the previous one
// expired). The handler encrypts BearerToken and upserts the row in
// execution_db.user_credentials so backend-only flows (the protective
// replayer at 15:35 IST, the live order path) always have a fresh token.
//
// Fields default from request headers when blank:
//
//	UserID        ← header "userId"
//	IndiraUserID  ← header "userId"  (Indira == platform user)
//	AppID         ← header "appId"
//	Source        ← header "source"
//
// BearerToken has no header fallback — it must be provided in the body so
// we never persist whatever JWT the caller happens to be authing this
// request with (which may be a session cookie's token, not the broker JWT).
type UpdateCredentialsRequest struct {
	UserID       string `json:"user_id,omitempty"`
	IndiraUserID string `json:"indira_user_id,omitempty"`
	AppID        string `json:"app_id,omitempty"`
	Source       string `json:"source,omitempty"`
	BearerToken  string `json:"bearer_token"`
}
