package consumer

import (
	"os"
	"testing"
)

// The compliance ceilings can be switched off from config (env var = 0) so a
// guard can be disabled and later re-enabled without a code change. These tests
// pin that contract, in particular the part that is easy to get wrong: a typo or
// a negative value must NOT be read as "disabled".

func TestEnvLimitInt32(t *testing.T) {
	const key = "TEST_LIMIT_INT"
	tests := []struct {
		name string
		set  bool
		val  string
		want int32
	}{
		{"unset keeps default", false, "", 50000},
		{"empty keeps default", true, "", 50000},
		{"explicit zero disables", true, "0", 0},
		{"positive overrides", true, "1234", 1234},
		{"whitespace tolerated", true, "  777  ", 777},
		// The important cases: bad input must fall back to the safe default,
		// never to 0, or a typo would silently remove a safety ceiling.
		{"malformed keeps default", true, "abc", 50000},
		{"negative keeps default", true, "-5", 50000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			os.Unsetenv(key)
			if tc.set {
				os.Setenv(key, tc.val)
				defer os.Unsetenv(key)
			}
			if got := envLimitInt32(key, 50000); got != tc.want {
				t.Fatalf("envLimitInt32 = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestEnvLimitFloat(t *testing.T) {
	const key = "TEST_LIMIT_FLOAT"
	tests := []struct {
		name string
		set  bool
		val  string
		want float64
	}{
		{"unset keeps default", false, "", 20000000},
		{"explicit zero disables", true, "0", 0},
		{"positive overrides", true, "150000.5", 150000.5},
		{"malformed keeps default", true, "1e", 20000000},
		{"negative keeps default", true, "-1", 20000000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			os.Unsetenv(key)
			if tc.set {
				os.Setenv(key, tc.val)
				defer os.Unsetenv(key)
			}
			if got := envLimitFloat(key, 20000000); got != tc.want {
				t.Fatalf("envLimitFloat = %v, want %v", got, tc.want)
			}
		})
	}
}

// envLimit* must differ from the plain envInt32/envFloat helpers precisely on
// the "0" case — that difference is the whole reason they exist. envInt32 treats
// 0 as invalid and restores the default, which made a ceiling impossible to
// disable from config.
func TestEnvLimitDiffersFromPlainHelperOnZero(t *testing.T) {
	const key = "TEST_ZERO_SEMANTICS"
	os.Setenv(key, "0")
	defer os.Unsetenv(key)

	if got := envInt32(key, 50000); got != 50000 {
		t.Fatalf("envInt32 with 0 = %d, want the default 50000 (unchanged behaviour)", got)
	}
	if got := envLimitInt32(key, 50000); got != 0 {
		t.Fatalf("envLimitInt32 with 0 = %d, want 0 (disabled)", got)
	}
}

// LoadComplianceLimitsFromEnv is the function main() calls after loading .env.
// Verify it actually applies a disable, and restores a limit when re-enabled —
// the round trip the operator will perform.
func TestLoadComplianceLimitsDisableAndReEnable(t *testing.T) {
	saveQty, saveVal, saveExp := maxOrderQuantity, maxOrderValueINR, maxExposureLimitINR
	t.Cleanup(func() {
		maxOrderQuantity, maxOrderValueINR, maxExposureLimitINR = saveQty, saveVal, saveExp
		os.Unsetenv("MAX_ORDER_QUANTITY")
		os.Unsetenv("MAX_ORDER_VALUE")
		os.Unsetenv("MAX_EXPOSURE_LIMIT")
	})

	os.Setenv("MAX_ORDER_QUANTITY", "0")
	os.Setenv("MAX_ORDER_VALUE", "0")
	os.Setenv("MAX_EXPOSURE_LIMIT", "0")
	LoadComplianceLimitsFromEnv()

	if maxOrderQuantity != 0 || maxOrderValueINR != 0 || maxExposureLimitINR != 0 {
		t.Fatalf("expected all three disabled, got qty=%d value=%v exposure=%v",
			maxOrderQuantity, maxOrderValueINR, maxExposureLimitINR)
	}

	// Re-enabling must be config-only — no rebuild, no code edit.
	os.Setenv("MAX_ORDER_QUANTITY", "5000")
	os.Setenv("MAX_ORDER_VALUE", "1000000")
	os.Setenv("MAX_EXPOSURE_LIMIT", "2000000")
	LoadComplianceLimitsFromEnv()

	if maxOrderQuantity != 5000 || maxOrderValueINR != 1000000 || maxExposureLimitINR != 2000000 {
		t.Fatalf("re-enable failed: qty=%d value=%v exposure=%v",
			maxOrderQuantity, maxOrderValueINR, maxExposureLimitINR)
	}
}

func TestLimitLabels(t *testing.T) {
	if got := limitLabelInt(0); got != "DISABLED" {
		t.Fatalf("limitLabelInt(0) = %q", got)
	}
	if got := limitLabelInt(50000); got != "50000" {
		t.Fatalf("limitLabelInt(50000) = %q", got)
	}
	if got := limitLabelFloat(0); got != "DISABLED" {
		t.Fatalf("limitLabelFloat(0) = %q", got)
	}
	if got := limitLabelFloat(20000000); got != "20000000" {
		t.Fatalf("limitLabelFloat(20000000) = %q", got)
	}
}
