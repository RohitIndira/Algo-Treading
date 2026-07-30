package config

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

// clearInsecureFlag makes sure ALLOW_INSECURE_ENCRYPTION_KEY is unset for a
// test unless the test explicitly sets it. t.Setenv registers cleanup, so the
// original process env is restored after the test.
func clearInsecureFlag(t *testing.T) {
	t.Helper()
	t.Setenv("ALLOW_INSECURE_ENCRYPTION_KEY", "")
}

func TestLoad_PlaceholderKey_NoFlag_Errors(t *testing.T) {
	clearInsecureFlag(t)
	t.Setenv("ENCRYPTION_KEY", insecureDefaultEncryptionKey)

	cfg, err := Load()
	if err == nil {
		t.Fatalf("expected error for placeholder key without ALLOW flag, got cfg=%+v", cfg)
	}
	if !strings.Contains(err.Error(), "insecure built-in default") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestLoad_PlaceholderKey_WithFlag_OKAndWarns(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", insecureDefaultEncryptionKey)
	t.Setenv("ALLOW_INSECURE_ENCRYPTION_KEY", "true")

	// Capture the WARN emitted via the standard logger.
	var buf bytes.Buffer
	prevOut := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error with ALLOW flag set, got %v", err)
	}
	if cfg.EncryptionKey != insecureDefaultEncryptionKey {
		t.Fatalf("expected placeholder key to be used, got %q", cfg.EncryptionKey)
	}
	if !strings.Contains(buf.String(), "ALLOW_INSECURE_ENCRYPTION_KEY") {
		t.Fatalf("expected WARN log about insecure key, got: %q", buf.String())
	}
}

func TestLoad_RealKey_OKNoWarn(t *testing.T) {
	clearInsecureFlag(t)
	realKey := "an-actual-32-byte-long-secret!!!" // 32 bytes
	if len(realKey) != 32 {
		t.Fatalf("test key must be 32 bytes, got %d", len(realKey))
	}
	t.Setenv("ENCRYPTION_KEY", realKey)

	var buf bytes.Buffer
	prevOut := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prevOut)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error for a real 32-byte key, got %v", err)
	}
	if cfg.EncryptionKey != realKey {
		t.Fatalf("expected real key to be used, got %q", cfg.EncryptionKey)
	}
	if strings.Contains(buf.String(), "ALLOW_INSECURE_ENCRYPTION_KEY") {
		t.Fatalf("did not expect an insecure-key WARN for a real key, got: %q", buf.String())
	}
}

func TestLoad_ShortKey_Errors(t *testing.T) {
	clearInsecureFlag(t)
	t.Setenv("ENCRYPTION_KEY", "20-char-key-exactly!") // 20 bytes → invalid

	cfg, err := Load()
	if err == nil {
		t.Fatalf("expected error for 20-byte key, got cfg=%+v", cfg)
	}
	if !strings.Contains(err.Error(), "16, 24, or 32") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
