package checker

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/risk_management"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/repository"
)

type PreTradeChecker struct {
	redisRepo *repository.RedisRepository
}

func NewPreTradeChecker(redisRepo *repository.RedisRepository) *PreTradeChecker {
	return &PreTradeChecker{redisRepo: redisRepo}
}

// applyProfileAdjustments modifies limits based on user psychology/risk profile
func (p *PreTradeChecker) applyProfileAdjustments(profile string, req *pb.PreTradeRiskRequest) *pb.PreTradeRiskRequest {
	prof := strings.ToLower(strings.TrimSpace(profile))
	// Create new instance to avoid copying mutex
	out := &pb.PreTradeRiskRequest{
		UserId:          req.UserId,
		StrategyId:      req.StrategyId,
		StockCode:       req.StockCode,
		Exchange:        req.Exchange,
		OrderType:       req.OrderType,
		OrderSide:       req.OrderSide,
		Quantity:        req.Quantity,
		Price:           req.Price,
		StopLoss:        req.StopLoss,
		TakeProfit:      req.TakeProfit,
		MaxDailyTrades:  req.MaxDailyTrades,
		MaxLossPerDay:   req.MaxLossPerDay,
		MaxPositionSize: req.MaxPositionSize,
		MaxPerTradeRisk: req.MaxPerTradeRisk,
	}

	switch prof {
	case "conservative":
		// tighten limits
		out.MaxDailyTrades = int32(float64(req.MaxDailyTrades) * 0.5)
		out.MaxPositionSize = req.MaxPositionSize * 0.5
		out.MaxPerTradeRisk = req.MaxPerTradeRisk * 0.5
	case "aggressive":
		// loosen limits
		out.MaxDailyTrades = int32(float64(req.MaxDailyTrades) * 1.5)
		out.MaxPositionSize = req.MaxPositionSize * 1.5
		out.MaxPerTradeRisk = req.MaxPerTradeRisk * 1.5
	default:
		// moderate or unknown -> no change
	}
	return out
}

// fingerprintOrder returns a simple fingerprint used for duplicate detection
func fingerprintOrder(req *pb.PreTradeRiskRequest) string {
	return fmt.Sprintf("%d:%d:%d", req.StockCode, int(req.OrderSide), req.Quantity)
}

// CheckPreTrade performs all pre-trade risk checks including 8 checks
func (p *PreTradeChecker) CheckPreTrade(ctx context.Context, req *pb.PreTradeRiskRequest) (*pb.PreTradeRiskResponse, error) {
	violations := []*pb.RiskViolation{}
	riskScore := 0.0

	// fetch user risk profile from Redis (psychology)
	profile, _ := p.redisRepo.GetRiskProfile(ctx, req.UserId)
	adjReq := p.applyProfileAdjustments(profile, req)

	// 1. Daily Trade Limit
	tradeCount, err := p.redisRepo.GetTodayTradeCount(ctx, req.UserId)
	if err == nil && tradeCount >= int64(adjReq.MaxDailyTrades) {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_DAILY_TRADE_LIMIT,
			Message:      "Daily trade limit exceeded",
			CurrentValue: float64(tradeCount),
			LimitValue:   float64(adjReq.MaxDailyTrades),
		})
		riskScore += 15.0
	}

	// 2. Daily Loss Limit
	dailyLoss, err := p.redisRepo.GetDailyLoss(ctx, req.UserId)
	if err == nil && dailyLoss > adjReq.MaxLossPerDay {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_DAILY_LOSS_LIMIT,
			Message:      "Daily loss limit exceeded",
			CurrentValue: dailyLoss,
			LimitValue:   adjReq.MaxLossPerDay,
		})
		riskScore += 20.0
	}

	// 3. Position Size Limit
	positionValue := float64(req.Quantity) * req.Price
	if positionValue > adjReq.MaxPositionSize {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_POSITION_SIZE_LIMIT,
			Message:      "Position size exceeds limit",
			CurrentValue: positionValue,
			LimitValue:   adjReq.MaxPositionSize,
		})
		riskScore += 10.0
	}

	// 4. Per-Trade Risk Limit
	potentialLoss := float64(req.Quantity) * (req.Price - req.StopLoss)
	if potentialLoss > adjReq.MaxPerTradeRisk {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_PER_TRADE_RISK_LIMIT,
			Message:      "Per-trade risk exceeds limit",
			CurrentValue: potentialLoss,
			LimitValue:   adjReq.MaxPerTradeRisk,
		})
		riskScore += 15.0
	}

	// 5. Duplicate Order Prevention
	fingerprint := fingerprintOrder(req)
	dup, err := p.redisRepo.CheckDuplicateOrder(ctx, req.UserId, fingerprint)
	if err == nil && dup {
		violations = append(violations, &pb.RiskViolation{
			Type:    pb.RiskViolationType_RISK_VIOLATION_DUPLICATE_ORDER,
			Message: "Duplicate order detected within configured window",
		})
		riskScore += 10.0
	}

	// 6. Insufficient Margin Check
	balance, err := p.redisRepo.GetUserBalance(ctx, req.UserId)
	requiredMargin := positionValue // simple approximation
	if err == nil && balance < requiredMargin {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_INSUFFICIENT_MARGIN,
			Message:      "Insufficient margin",
			CurrentValue: balance,
			LimitValue:   requiredMargin,
		})
		riskScore += 20.0
	}

	// 7. Circuit Breaker Check (daily loss pct)
	// attempt to read portfolio value from Redis; fallback to balance
	portfolioValue := balance
	// Use default 5% circuit breaker threshold since field doesn't exist in proto
	circuitBreakerThreshold := 5.0
	if portfolioValue > 0 && dailyLoss/portfolioValue*100.0 > circuitBreakerThreshold {
		violations = append(violations, &pb.RiskViolation{
			Type:    pb.RiskViolationType_RISK_VIOLATION_CIRCUIT_BREAKER,
			Message: "Circuit breaker triggered (excessive loss %)",
		})
		riskScore += 25.0
	}

	// 8. Concentration Limit Check
	// Use default 20% concentration limit since field doesn't exist in proto
	maxConcentrationPct := 20.0
	// compute largest position percent using stored positions
	keys, _ := p.redisRepo.GetUserPositionsKeys(ctx, req.UserId)
	largestPct := 0.0
	total := portfolioValue
	// best-effort: iterate positions to calculate current exposure
	for range keys {
		// each key ends with :{stock}
		h, err := p.redisRepo.GetPositionHash(ctx, req.UserId, 0)
		if err != nil {
			continue
		}
		// try to parse quantity and avg_price
		qtyStr := h["quantity"]
		avgStr := h["avg_price"]
		// simple parse with fmt.Sscanf
		var qty int
		var avg float64
		fmt.Sscanf(qtyStr, "%d", &qty)
		fmt.Sscanf(avgStr, "%f", &avg)
		posVal := float64(qty) * avg
		if total > 0 {
			pct := posVal / total * 100.0
			if pct > largestPct {
				largestPct = pct
			}
		}
	}
	newPosPct := (positionValue / (total + positionValue)) * 100.0
	if newPosPct > maxConcentrationPct || largestPct > maxConcentrationPct {
		violations = append(violations, &pb.RiskViolation{
			Type:         pb.RiskViolationType_RISK_VIOLATION_CONCENTRATION_LIMIT,
			Message:      "Concentration limit exceeded",
			CurrentValue: newPosPct,
			LimitValue:   maxConcentrationPct,
		})
		riskScore += 10.0
	}

	approved := len(violations) == 0

	// if approved and not duplicate, register duplicate fingerprint
	if approved {
		_ = p.redisRepo.AddDuplicateOrder(ctx, req.UserId, fingerprint, "", 30)
	}

	return &pb.PreTradeRiskResponse{
		Approved:    approved,
		Violations:  violations,
		RiskScore:   riskScore,
		Suggestions: p.generateSuggestions(violations),
	}, nil
}

func (p *PreTradeChecker) generateSuggestions(violations []*pb.RiskViolation) []string {
	suggestions := []string{}
	for _, v := range violations {
		switch v.Type {
		case pb.RiskViolationType_RISK_VIOLATION_DAILY_TRADE_LIMIT:
			suggestions = append(suggestions, "Reduce trading frequency or review daily trade limit in user settings")
		case pb.RiskViolationType_RISK_VIOLATION_DAILY_LOSS_LIMIT:
			suggestions = append(suggestions, "Stop trading for today; consider adjusting strategy risk")
		case pb.RiskViolationType_RISK_VIOLATION_POSITION_SIZE_LIMIT:
			suggestions = append(suggestions, "Lower order quantity or split orders")
		case pb.RiskViolationType_RISK_VIOLATION_PER_TRADE_RISK_LIMIT:
			suggestions = append(suggestions, "Increase stop loss distance or reduce quantity")
		case pb.RiskViolationType_RISK_VIOLATION_DUPLICATE_ORDER:
			suggestions = append(suggestions, "Wait and retry or cancel duplicate order")
		case pb.RiskViolationType_RISK_VIOLATION_INSUFFICIENT_MARGIN:
			suggestions = append(suggestions, "Deposit more funds or reduce order size")
		case pb.RiskViolationType_RISK_VIOLATION_CIRCUIT_BREAKER:
			suggestions = append(suggestions, "Trading halted due to circuit breaker")
		case pb.RiskViolationType_RISK_VIOLATION_CONCENTRATION_LIMIT:
			suggestions = append(suggestions, "Reduce concentration by trading other instruments or reducing size")
		}
	}
	return suggestions
}
