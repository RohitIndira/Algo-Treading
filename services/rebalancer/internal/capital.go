package internal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/RohitIndira/Algo-Treading/pkg/crypto"
	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// encryptionKey returns the AES key used by the trade-execution credentials
// repository when storing tokens. We MUST use the same key here since we
// read the encrypted ciphertext directly from trading_execution.user_credentials.
//
// Default matches what trade-execution's main.go uses if ENCRYPTION_KEY is
// not set in env. In production both services should share the same key
// via .env or secrets manager.
func encryptionKey() string {
	if k := os.Getenv("ENCRYPTION_KEY"); k != "" {
		return k
	}
	return "0123456789abcdef0123456789abcdef"
}

// FetchCurrentCapital pulls real-time capital from the broker for one user.
//
// Sources (all called against Indira via pkg/indira):
//
//   1. fund-limit  → AvailableMargin (cash sitting at broker)
//   2. positions   → today's CNC delivery positions (NetQty × LTP).
//                    MIS / INTRADAY rows are skipped — they're a separate
//                    book from our positional Manthan capital.
//   3. holdings    → settled CNC positions from prior days (Qty × CurrentPrice)
//
// Total capital = cash + positions value + holdings value.
//
// Falls back to manthan_portfolio_state.current_capital if any broker call
// fails — better to rebalance with stale data than not at all. Drift between
// broker and DB is logged for later investigation.
func FetchCurrentCapital(
	ctx context.Context,
	client *indiraClient.Client,
	auth *indiraClient.AuthContext,
	db *sql.DB,
	strategyID string,
	logger *zap.Logger,
) CurrentCapital {
	cap := CurrentCapital{Source: "broker", Prices: map[string]float64{}}

	// Always read DB snapshot — used for cross-check or fallback.
	if db != nil {
		_ = db.QueryRowContext(ctx,
			`SELECT current_capital FROM manthan_portfolio_state WHERE strategy_id = $1`,
			strategyID).Scan(&cap.DBStateSnapshot)
	}

	// 1. Fund-limit (cash)
	fl, err := client.GetFundLimit(ctx, auth)
	if err != nil {
		logger.Warn("FetchCurrentCapital: fund-limit failed — falling back to DB",
			zap.String("strategy", strategyID), zap.Error(err))
		return useDBFallback(cap, err)
	}
	cap.Cash = fl.AvailableMargin

	// 2. Position-book (today's delivery positions)
	positions, err := client.GetPositions(ctx, auth)
	if err != nil {
		logger.Warn("FetchCurrentCapital: positions failed — falling back to DB",
			zap.String("strategy", strategyID), zap.Error(err))
		return useDBFallback(cap, err)
	}
	for _, p := range positions {
		// Skip intraday rows (MIS short doesn't count toward our long capital).
		if !isDelivery(p.PrdType) {
			continue
		}
		// Skip non-stock instruments defensively.
		if p.Symbol.Instrument != "" {
			ins := strings.ToUpper(p.Symbol.Instrument)
			if ins != "STK" && ins != "EQ" {
				continue
			}
		}
		if p.NetQty == 0 {
			continue
		}
		cap.PositionsValue += float64(p.NetQty) * p.LTP
		// Cache per-symbol LTP for the top-up phase.
		if sym := strings.ToUpper(strings.TrimSpace(p.Symbol.DispSym)); sym != "" && p.LTP > 0 {
			cap.Prices[sym] = p.LTP
		}
	}

	// 3. Holdings (settled prior-day positions)
	holdings, err := client.GetHoldings(ctx, auth)
	if err != nil {
		logger.Warn("FetchCurrentCapital: holdings failed — using cash + positions only",
			zap.String("strategy", strategyID), zap.Error(err))
		// Don't fall all the way back — partial broker data is still better
		// than DB. Just leave HoldingsValue at zero.
	} else {
		for _, h := range holdings {
			if h.Qty <= 0 {
				continue
			}
			// Use LTP (the new field name matching the Indira API doc); the
			// old "currentPrice" field was a mis-named legacy that the broker
			// never actually populated.
			cap.HoldingsValue += float64(h.Qty) * h.LTP
		}
	}

	cap.Total = cap.Cash + cap.PositionsValue + cap.HoldingsValue

	// Drift detection: log if broker truth disagrees with our DB by >5%.
	if cap.DBStateSnapshot > 0 {
		drift := cap.Total - cap.DBStateSnapshot
		driftPct := drift / cap.DBStateSnapshot
		if driftPct > 0.05 || driftPct < -0.05 {
			logger.Warn("FetchCurrentCapital: drift between broker and DB > 5%",
				zap.String("strategy", strategyID),
				zap.Float64("broker_total", cap.Total),
				zap.Float64("db_state", cap.DBStateSnapshot),
				zap.Float64("drift_pct", driftPct*100))
		}
	}

	return cap
}

// useDBFallback returns a CurrentCapital wrapping the DB snapshot. Caller
// must ensure cap.DBStateSnapshot was already loaded.
func useDBFallback(cap CurrentCapital, err error) CurrentCapital {
	cap.Source = "db_fallback"
	cap.BrokerFetchError = err
	cap.Total = cap.DBStateSnapshot
	if cap.Total <= 0 {
		// No broker, no DB — nothing to allocate. Caller should treat
		// PerCall==0 as "skip this strategy" with a clear log line.
		cap.Total = 0
	}
	return cap
}

// isDelivery duplicates the same predicate used by the manual-activity
// detector — we deliberately do NOT import from trade-execution to keep
// services decoupled. Both implementations should accept the same set.
func isDelivery(prdType string) bool {
	switch strings.ToUpper(strings.TrimSpace(prdType)) {
	case "DELIVERY", "CNC", "DEL", "":
		return true
	}
	return false
}

// ResolveBrokerAuth pulls the bearer token + appId + source for a user
// from trading_execution.user_credentials. Returns nil if not found —
// caller should skip the strategy with a clear log line (no auth = no
// broker calls = no rebalance).
//
// IMPORTANT: trade-execution's credentials_repository encrypts bearer
// tokens with AES-256 before storing. We MUST decrypt here using the
// same key (ENCRYPTION_KEY env, default matches trade-execution).
// Without decryption the broker rejects every call with AU004 because
// the ciphertext fails JWT signature verification.
func ResolveBrokerAuth(ctx context.Context, execDB *sql.DB, userID string) (*indiraClient.AuthContext, error) {
	if execDB == nil || userID == "" {
		return nil, fmt.Errorf("ResolveBrokerAuth: missing inputs")
	}
	var encToken, appID, source string
	err := execDB.QueryRowContext(ctx, `
		SELECT indira_bearer_token, indira_app_id, indira_source
		FROM user_credentials
		WHERE user_id = $1`, userID).Scan(&encToken, &appID, &source)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("user_credentials lookup: %w", err)
	}
	plainToken, decErr := crypto.Decrypt(encToken, encryptionKey())
	if decErr != nil {
		return nil, fmt.Errorf("decrypt bearer token (check ENCRYPTION_KEY matches trade-execution): %w", decErr)
	}
	return &indiraClient.AuthContext{
		UserId:      userID,
		BearerToken: plainToken,
		AppId:       appID,
		Source:      source,
	}, nil
}
