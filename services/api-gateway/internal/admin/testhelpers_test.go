package admin

import (
	"context"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
)

// testAuthContext injects platform-auth claims exactly as the production
// middleware does (auth.WithClaims), so handleElevate's identity extraction
// is exercised for real rather than stubbed.
func testAuthContext(ctx context.Context, userID string) context.Context {
	return auth.WithClaims(ctx, &auth.Claims{UserID: userID})
}
