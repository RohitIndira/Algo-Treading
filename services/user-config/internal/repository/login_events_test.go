package repository

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func fakeJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	b, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	seg := base64.RawURLEncoding.EncodeToString
	return seg([]byte(`{"alg":"HS512","typ":"JWT"}`)) + "." + seg(b) + "." + seg([]byte("sig"))
}

func TestParseLoginClaims(t *testing.T) {
	tok := fakeJWT(t, map[string]any{"iat": 1793257200, "exp": 1793300400, "loginSource": "sso", "userId": "S4450"})
	c := parseLoginClaims(tok)
	if got := c.IssuedAt.Unix(); got != 1793257200 {
		t.Fatalf("iat = %d", got)
	}
	if got := c.ExpiresAt.Unix(); got != 1793300400 {
		t.Fatalf("exp = %d", got)
	}
	if c.LoginSource != "SSO" {
		t.Fatalf("loginSource = %q", c.LoginSource)
	}

	// No claims we care about → zero values, no panic.
	c = parseLoginClaims(fakeJWT(t, map[string]any{"userId": "X"}))
	if !c.IssuedAt.IsZero() || !c.ExpiresAt.IsZero() || c.LoginSource != "" {
		t.Fatalf("expected empty claims, got %s", c)
	}

	// Garbage tokens are tolerated (a login is still a login).
	for _, bad := range []string{"", "nodots", "a.b", "a.!!!.c", "a." + base64.RawURLEncoding.EncodeToString([]byte("not json")) + ".c"} {
		if c := parseLoginClaims(bad); !c.IssuedAt.IsZero() {
			t.Fatalf("%q parsed to %s", bad, c)
		}
	}
}

func TestTokenFingerprint(t *testing.T) {
	a, b := tokenFingerprint("tokenA"), tokenFingerprint("tokenB")
	if a == b || len(a) != 24 || tokenFingerprint("tokenA") != a {
		t.Fatalf("fingerprint unstable or colliding: %s %s", a, b)
	}
}

// DB-backed: the same token twice is ONE login; a new token is a second row;
// the credential upsert itself is unaffected. Needs the local execution_db.
func TestStoreIndiraCredentials_RecordsLoginHistory(t *testing.T) {
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=execution_db sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("execution_db not reachable — skipping (%v)", err)
	}
	t.Cleanup(func() { db.Close() })
	mig, err := os.ReadFile(filepath.Join("..", "..", "..", "trade-execution", "migrations", "018_user_login_events.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(mig)); err != nil {
		t.Fatalf("apply 018: %v", err)
	}
	const user = "TUC_LOGIN_HIST"
	clean := func() {
		_, _ = db.Exec(`DELETE FROM user_login_events WHERE user_id = $1`, user)
		_, _ = db.Exec(`DELETE FROM user_credentials WHERE user_id = $1`, user)
	}
	clean()
	t.Cleanup(clean)

	repo := NewCredentialsRepository(sqlx.NewDb(db, "postgres"), "0123456789abcdef0123456789abcdef")
	ctx := context.Background()
	tok1 := fakeJWT(t, map[string]any{"iat": 1793257200, "exp": 1793300400, "loginSource": "SSO"})
	tok2 := fakeJWT(t, map[string]any{"iat": 1793260800, "exp": 1793304000, "loginSource": "APP"})

	for _, tok := range []string{tok1, tok1, tok2} {
		if err := repo.StoreIndiraCredentials(ctx, user, user, "app", "SSO", tok); err != nil {
			t.Fatalf("store: %v", err)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM user_login_events WHERE user_id = $1 AND kind = 'LOGIN'`, user).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2 login rows (tok1 deduped), got %d", n)
	}
	var iat time.Time
	var ls string
	if err := db.QueryRow(`SELECT token_issued_at, login_source FROM user_login_events WHERE user_id = $1 ORDER BY id DESC LIMIT 1`, user).Scan(&iat, &ls); err != nil {
		t.Fatal(err)
	}
	if iat.Unix() != 1793260800 || ls != "APP" {
		t.Fatalf("latest row iat=%d loginSource=%s", iat.Unix(), ls)
	}
	// The credential row holds the newest token regardless of history.
	got, err := repo.GetIndiraCredentials(ctx, user)
	if err != nil || got.BearerToken != tok2 {
		t.Fatalf("credential readback: %v %+v", err, got)
	}
}
