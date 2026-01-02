package matcher

import "fmt"

// Scorer calculates match scores based on evaluation results
type Scorer struct {
	// Weights for different conditions (total should be 100)
	weights map[string]float64
}

// NewScorer creates a new scorer with default weights for market depth trading
func NewScorer() *Scorer {
	return &Scorer{
		weights: map[string]float64{
			"market_depth": 40.0, // Primary signal - market depth conditions
			"stock":        20.0, // Stock selection
			"price_range":  15.0, // Price must be in range
			"volume":       10.0, // Volume threshold
			"exchange":     10.0, // Exchange preference
			"pct_change":   5.0,  // Optional percent change filter
		},
	}
}

// NewScorerWithWeights creates a scorer with custom weights
func NewScorerWithWeights(weights map[string]float64) *Scorer {
	return &Scorer{
		weights: weights,
	}
}

// CalculateScore calculates the overall match score
func (s *Scorer) CalculateScore(result *EvaluationResult) float64 {
	if result == nil {
		return 0
	}

	totalScore := 0.0
	totalWeight := 0.0

	// Calculate weighted score for each condition
	for condition, conditionScore := range result.ConditionScores {
		weight := s.weights[condition]
		if weight == 0 {
			// Default weight if not specified
			weight = 10.0
		}

		// Add weighted score
		totalScore += conditionScore * (weight / 100.0)
		totalWeight += weight
	}

	// Normalize to 0-100 scale
	if totalWeight == 0 {
		return 0
	}

	normalizedScore := (totalScore / totalWeight) * 100.0

	// Apply bonus for full matches (all conditions met)
	if result.IsFullMatch() {
		normalizedScore = min(normalizedScore*1.1, 100.0) // 10% bonus capped at 100
	}

	// Apply penalty for many failed conditions
	failureRate := float64(result.GetFailedConditionCount()) / float64(result.GetTotalConditionCount())
	if failureRate > 0.5 { // More than 50% conditions failed
		normalizedScore *= (1.0 - failureRate*0.2) // Up to 10% penalty
	}

	return normalizedScore
}

// CalculateWeightedScore calculates score with custom weights for this evaluation
func (s *Scorer) CalculateWeightedScore(result *EvaluationResult, customWeights map[string]float64) float64 {
	if result == nil {
		return 0
	}

	totalScore := 0.0
	totalWeight := 0.0

	for condition, conditionScore := range result.ConditionScores {
		weight := customWeights[condition]
		if weight == 0 {
			// Fall back to default weight
			weight = s.weights[condition]
			if weight == 0 {
				weight = 10.0
			}
		}

		totalScore += conditionScore * (weight / 100.0)
		totalWeight += weight
	}

	if totalWeight == 0 {
		return 0
	}

	return (totalScore / totalWeight) * 100.0
}

// GetWeight returns the weight for a condition
func (s *Scorer) GetWeight(condition string) float64 {
	return s.weights[condition]
}

// SetWeight sets the weight for a condition
func (s *Scorer) SetWeight(condition string, weight float64) {
	s.weights[condition] = weight
}

// GetWeights returns all weights
func (s *Scorer) GetWeights() map[string]float64 {
	// Return a copy to prevent external modification
	weights := make(map[string]float64, len(s.weights))
	for k, v := range s.weights {
		weights[k] = v
	}
	return weights
}

// ValidateWeights checks if weights are reasonable
func (s *Scorer) ValidateWeights() error {
	totalWeight := 0.0
	for _, weight := range s.weights {
		if weight < 0 {
			return ErrInvalidWeight
		}
		totalWeight += weight
	}

	// Warn if weights don't sum to 100 (but don't error)
	if totalWeight < 90 || totalWeight > 110 {
		// Weights significantly off from 100
		return ErrWeightSumMismatch
	}

	return nil
}

// Helper function for min
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// Errors
var (
	ErrInvalidWeight     = fmt.Errorf("weight cannot be negative")
	ErrWeightSumMismatch = fmt.Errorf("weights should sum to approximately 100")
)
