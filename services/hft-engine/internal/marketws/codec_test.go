// Unit tests for the ODIN broadcast wire codec.
//
// Strategy:
//   1. Round-trip: encode("foo|bar") → decode → "foo|bar"
//   2. Parse a real-looking 209 frame (FIX payload taken from the spec)
//      and assert every field comes out right.
//   3. Multi-message concatenated payload splits correctly.
//   4. Login response decoder maps tag 70 → status, etc.
//
// We don't test against a captured-from-the-wire frame here because we
// don't have one yet — when one arrives, paste its hex into TestParseLive209
// and we lock the implementation against it.
package marketws

import (
	"strings"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────
// Encode/decode round-trip
// ─────────────────────────────────────────────────────────────────────────

func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := "63=FT3.0|64=101|65=74|66=14:59:22|67=USER1|68=APIKEY|4=|400=0|401=2|396=HO|51=4|395=127.0.0.1"
	frame, err := EncodeFix(original)
	if err != nil {
		t.Fatalf("EncodeFix: %v", err)
	}
	if len(frame) < 10 {
		t.Fatalf("frame suspiciously short: %d bytes", len(frame))
	}
	if frame[0] != frameMarkerCompressed {
		t.Fatalf("first byte should be 0x05, got 0x%02x", frame[0])
	}
	// Trailer must be 4 null bytes.
	if frame[len(frame)-4] != 0 || frame[len(frame)-1] != 0 {
		t.Fatal("trailing 4 bytes should be null")
	}

	decoded, err := DecodeFrame(frame)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if decoded != original {
		t.Fatalf("round-trip mismatch:\n  want: %q\n  got:  %q", original, decoded)
	}
}

func TestEncode_LargePayload(t *testing.T) {
	// 100 fake tag=value pairs — exercises larger frames.
	var b strings.Builder
	for i := 0; i < 100; i++ {
		if i > 0 {
			b.WriteString("|")
		}
		b.WriteString("10")
		b.WriteString(strings.Repeat("0", 3))
		b.WriteString("=VALUE-")
		b.WriteString(strings.Repeat("X", 50))
	}
	payload := b.String()
	frame, err := EncodeFix(payload)
	if err != nil {
		t.Fatalf("EncodeFix: %v", err)
	}
	got, err := DecodeFrame(frame)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if got != payload {
		t.Fatalf("large round-trip failed (got %d bytes, want %d)", len(got), len(payload))
	}
}

// ─────────────────────────────────────────────────────────────────────────
// FIX parser
// ─────────────────────────────────────────────────────────────────────────

func TestParseFix_BasicTags(t *testing.T) {
	payload := "63=FT3.0|64=209|65=272|66=04092012 071138|1=1|7=22|3=130035|6=130100|8=130035|9=395|74=2012-09-04 152908"
	m, code := ParseFix(payload)

	if code != 209 {
		t.Fatalf("expected code 209, got %d", code)
	}
	if got := m.Str(63, ""); got != "FT3.0" {
		t.Fatalf("tag 63: want FT3.0, got %q", got)
	}
	if got := m.Int(1, -1); got != 1 {
		t.Fatalf("tag 1 (segment): want 1, got %d", got)
	}
	if got := m.Int64(7, -1); got != 22 {
		t.Fatalf("tag 7 (security_code): want 22, got %d", got)
	}
}

func TestParseFix_MalformedFieldsSkipped(t *testing.T) {
	// One field has no '=' (bad), one has empty value (bad), one is normal.
	payload := "63=FT3.0|64=209|BADFIELD|emptyvaluetag=|1=1|7=22"
	m, code := ParseFix(payload)
	if code != 209 {
		t.Fatalf("code should still be 209 despite bad fields, got %d", code)
	}
	if m.Int(1, -1) != 1 || m.Int(7, -1) != 22 {
		t.Fatal("good tags should still parse after bad ones")
	}
	if _, exists := m[0]; exists {
		t.Fatal("malformed BADFIELD should not have been parsed")
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Typed converters
// ─────────────────────────────────────────────────────────────────────────

func TestAsTouchline_HappyPath(t *testing.T) {
	// Taken from the ODIN spec example (page 12).
	payload := "63=FT3.0|64=209|65=272|66=04092012 071138|1=1|73=2012-09-04 152908|7=22|79=121758|8=130035|54=-0.63|9=395|80=130117|74=2012-09-04 152908|2=5|3=130035|5=59|6=130100|81=99436|82=82671|76=130860|75=130770|77=130770|78=129615"
	m, _ := ParseFix(payload)
	tl := m.AsTouchline()
	if tl == nil {
		t.Fatal("AsTouchline returned nil on a valid 209")
	}

	cases := []struct {
		name    string
		got     int64
		want    int64
	}{
		{"SegmentID", int64(tl.SegmentID), 1},
		{"SecurityCode", tl.SecurityCode, 22},
		{"BidQty", tl.BidQty, 5},
		{"BidPaise", tl.BidPaise, 130035},
		{"AskQty", tl.AskQty, 59},
		{"AskPaise", tl.AskPaise, 130100},
		{"LTPPaise", tl.LTPPaise, 130035},
		{"LTPQty", tl.LTPQty, 395},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: want %d, got %d", c.name, c.want, c.got)
		}
	}
	if tl.LastUpdateTime != "2012-09-04 152908" {
		t.Errorf("LastUpdateTime: %q", tl.LastUpdateTime)
	}
}

func TestAsTouchline_WrongCodeReturnsNil(t *testing.T) {
	// Logon-response code; AsTouchline should refuse.
	m, _ := ParseFix("63=FT3.0|64=102|70=10000|1=1")
	if tl := m.AsTouchline(); tl != nil {
		t.Fatal("AsTouchline should return nil for non-209 messages")
	}
}

func TestAsLogonResponse_Success(t *testing.T) {
	m, _ := ParseFix("63=FT3.0|64=102|65=22|66=20240115 100000|70=10000|1=1")
	resp := m.AsLogonResponse()
	if resp == nil {
		t.Fatal("AsLogonResponse returned nil on a valid 102")
	}
	if resp.LogonStatus != 10000 {
		t.Errorf("LogonStatus: want 10000, got %d", resp.LogonStatus)
	}
	if resp.SegmentID != 1 {
		t.Errorf("SegmentID: want 1, got %d", resp.SegmentID)
	}
}

func TestAsLogonResponse_AboutToExpire(t *testing.T) {
	m, _ := ParseFix("63=FT3.0|64=102|70=10004|97=7|1=1")
	resp := m.AsLogonResponse()
	if resp.LogonStatus != 10004 || resp.DaysToExpire != 7 {
		t.Errorf("LogonStatus=%d DaysToExpire=%d", resp.LogonStatus, resp.DaysToExpire)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Concatenated message split
// ─────────────────────────────────────────────────────────────────────────

func TestSplitConcatenated(t *testing.T) {
	a := "63=FT3.0|64=209|1=1|7=22"
	b := "63=FT3.0|64=209|1=1|7=193"
	// ODIN concatenation uses 0x02 between glued messages.
	combined := a + "\x02" + b

	parts := SplitConcatenated(combined)
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d: %#v", len(parts), parts)
	}
	if parts[0] != a {
		t.Errorf("part 0 mismatch: %q", parts[0])
	}
	if parts[1] != b {
		t.Errorf("part 1 mismatch: %q", parts[1])
	}
}

func TestSplitConcatenated_EmptyFragmentsDropped(t *testing.T) {
	// Trailing 0x02 + nulls (common in real frames).
	combined := "63=FT3.0|64=209|1=1\x02\x00\x00\x02"
	parts := SplitConcatenated(combined)
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d: %#v", len(parts), parts)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Decode-frame error paths
// ─────────────────────────────────────────────────────────────────────────

func TestDecodeFrame_TooShort(t *testing.T) {
	if _, err := DecodeFrame([]byte{0x05, 0x30}); err == nil {
		t.Fatal("expected error on 2-byte frame")
	}
}

func TestDecodeFrame_UnknownMarker(t *testing.T) {
	// 0x99 isn't a marker we recognise.
	bad := []byte{0x99, 0x30, 0x30, 0x30, 0x30, 0x30, 0xDE, 0xAD, 0xBE, 0xEF}
	if _, err := DecodeFrame(bad); err == nil {
		t.Fatal("expected error on unknown marker")
	}
}
