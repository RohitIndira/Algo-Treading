// Package marketws speaks ODIN's broadcast-feed wire protocol.
//
// codec.go has the pure encode/decode primitives — no I/O, no goroutines,
// no logger. Easy to unit-test, easy to swap implementations.
//
// The wire format (per the ODIN spec we received):
//
//   ┌──────────────────────────────────────────────────────────────────┐
//   │  Outbound frame (client → server):                               │
//   │                                                                  │
//   │    [1 byte: 0x05]              compression marker                │
//   │    [5 bytes ASCII]             zero-padded total length of       │
//   │                                  (length + compressed payload)   │
//   │    [N bytes]                   zlib-compressed FIX-like payload  │
//   │    [4 bytes: 0x00 × 4]         trailer                           │
//   │                                                                  │
//   │  The payload (after zlib.Inflate) is pipe-delimited tag=value:   │
//   │    "63=FT3.0|64=101|65=74|66=14:59:22|67=USER|68=APIKEY|..."     │
//   │                                                                  │
//   │  Inbound frame: same shape minus the trailer; multiple FIX       │
//   │  messages can be concatenated using 0x02 as separator.           │
//   └──────────────────────────────────────────────────────────────────┘
//
// Tag conventions we care about:
//
//   63 = FIX version          ("FIX3.0" or "FT3.0")
//   64 = Message code         (101 logon, 102 logon-resp, 206 sub, 209 touchline, 1111 disconnect)
//   65 = Message length       (length of message body, ignored on decode)
//   66 = Timestamp            ("HH:MM:SS" or "YYYY-MM-DD HH:MM:SS")
//
//   For 209 (Multiple Touchline Response):
//     1  = Segment ID         (1=NSE Cash, 2=NSE Derivatives, ...)
//     7  = Security Code      (exchange token, e.g. "193" for ARVIND on NSE)
//     2  = Buy Qty (best bid qty)
//     3  = Buy Price (best bid)   ← in paise; divide by 100 for ₹
//     5  = Sell Qty (best ask qty)
//     6  = Sell Price (best ask)  ← in paise
//     8  = Last Trade Price (LTP) ← in paise
//     74 = Last Update Time
//
// Price scaling: NSE Cash prices are quoted in paise (₹ × 100). We expose
// raw integers in TouchlineMsg and let the caller divide. The marketws
// fan-out applies the /100 conversion before pushing into the engine.
package marketws

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Compression markers in the byte stream.
const (
	frameMarkerCompressed   byte = 0x05 // first byte for zlib-compressed payload
	frameMarkerUncompressed byte = 0x02 // first byte / separator for plain payload
)

// EncodeFix takes a pipe-delimited FIX payload (e.g. a Logon request),
// zlib-compresses it, and wraps it in the ODIN broadcast frame.
//
//   [0x05] [5-char zero-padded length] [zlib payload] [4 × 0x00 trailer]
//
// The 4-byte null trailer is what the reference Python client appends —
// the ODIN feed handler treats it as a soft terminator. We replicate it
// for compatibility.
func EncodeFix(payload string) ([]byte, error) {
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, 7)
	if err != nil {
		return nil, fmt.Errorf("zlib new writer: %w", err)
	}
	if _, err := w.Write([]byte(payload)); err != nil {
		return nil, fmt.Errorf("zlib write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("zlib close: %w", err)
	}
	compressed := buf.Bytes()

	// Length field counts itself (5 chars) + zlib payload but NOT the
	// 0x05 marker — matching the Python reference implementation.
	totalLen := 5 + len(compressed) - 1
	lenStr := fmt.Sprintf("%05d", totalLen)
	if len(lenStr) != 5 {
		return nil, fmt.Errorf("encoded length overflow: payload too large")
	}

	out := make([]byte, 0, 1+5+len(compressed)+4)
	out = append(out, frameMarkerCompressed)
	out = append(out, []byte(lenStr)...)
	out = append(out, compressed...)
	out = append(out, 0, 0, 0, 0) // null trailer
	return out, nil
}

// DecodeFrame inverts EncodeFix on the recv path.
//
// Accepts both compressed (0x05 marker) and uncompressed (0x02 marker)
// frames — the ODIN feed mixes both depending on payload size.
//
// Returns the decoded ASCII payload. Multiple concatenated messages are
// returned as ONE string with 0x02 separators between them; the caller
// runs SplitConcatenated to split them.
func DecodeFrame(raw []byte) (string, error) {
	if len(raw) < 6 {
		return "", fmt.Errorf("frame too short (%d bytes)", len(raw))
	}
	marker := raw[0]
	// Bytes 1-5 are the length field; we don't trust it (the server is
	// sometimes off-by-one) and just consume from byte 6 onward.
	body := raw[6:]

	// Strip trailing nulls some servers append.
	body = bytes.TrimRight(body, "\x00")

	switch marker {
	case frameMarkerCompressed:
		r, err := zlib.NewReader(bytes.NewReader(body))
		if err != nil {
			return "", fmt.Errorf("zlib new reader: %w", err)
		}
		defer r.Close()
		decompressed, err := io.ReadAll(r)
		if err != nil {
			return "", fmt.Errorf("zlib decompress: %w", err)
		}
		return string(decompressed), nil

	case frameMarkerUncompressed:
		return string(body), nil

	default:
		return "", fmt.Errorf("unknown frame marker 0x%02x", marker)
	}
}

// SplitConcatenated returns the individual FIX messages from a payload
// that may contain several glued together with 0x02 separators.
//
// Empty fragments are dropped; trailing whitespace and nulls trimmed.
func SplitConcatenated(payload string) []string {
	parts := strings.Split(payload, "\x02")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimRight(p, "\x00 \r\n\t")
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// FixMessage is the parsed form of one tag=value | tag=value | ... blob.
// Tags are integers per FIX convention; values are kept as raw strings
// (callers convert with helpers like Int / Float64).
//
// Code returns the value of tag 64 (message code), or 0 if missing.
type FixMessage map[int]string

// ParseFix splits a payload like "63=FT3.0|64=209|7=22|3=130035|..." into
// a tag→value map. Returns the message code (tag 64) for convenience.
//
// Malformed pairs (missing '=' or non-int tag) are skipped silently so
// one bad field doesn't drop the whole message.
func ParseFix(payload string) (FixMessage, int) {
	m := FixMessage{}
	for _, kv := range strings.Split(payload, "|") {
		if kv == "" {
			continue
		}
		eq := strings.IndexByte(kv, '=')
		if eq < 1 || eq == len(kv)-1 {
			continue
		}
		tag, err := strconv.Atoi(kv[:eq])
		if err != nil {
			continue
		}
		m[tag] = kv[eq+1:]
	}
	return m, m.Code()
}

// Code returns the message code (tag 64).
func (m FixMessage) Code() int {
	if v, ok := m[64]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 0
}

// Int returns m[tag] parsed as int, or def if missing/unparseable.
func (m FixMessage) Int(tag, def int) int {
	if v, ok := m[tag]; ok {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

// Int64 same as Int but for int64. Useful for prices in paise.
func (m FixMessage) Int64(tag int, def int64) int64 {
	if v, ok := m[tag]; ok {
		if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return n
		}
	}
	return def
}

// Str returns m[tag] or def. Trims whitespace.
func (m FixMessage) Str(tag int, def string) string {
	if v, ok := m[tag]; ok {
		return strings.TrimSpace(v)
	}
	return def
}

// ─────────────────────────────────────────────────────────────────────────
// Typed message helpers — the only response types the engine cares about.
// ─────────────────────────────────────────────────────────────────────────

// TouchlineMsg is the decoded form of a 209 Multiple Touchline Response.
// All prices are in PAISE (raw integer from the wire). Divide by 100 for ₹.
type TouchlineMsg struct {
	SegmentID    int
	SecurityCode int64

	BidQty   int64
	BidPaise int64 // tag 3
	AskQty   int64
	AskPaise int64 // tag 6
	LTPPaise int64 // tag 8
	LTPQty   int64 // tag 9

	LastUpdateTime string // tag 74 (raw "YYYY-MM-DD HH:MM:SS")
}

// AsTouchline decodes a 209 message. Returns nil if the message isn't a 209
// or the required fields (segment, security_code) are missing.
func (m FixMessage) AsTouchline() *TouchlineMsg {
	if m.Code() != 209 {
		return nil
	}
	if _, ok := m[1]; !ok {
		return nil
	}
	if _, ok := m[7]; !ok {
		return nil
	}
	return &TouchlineMsg{
		SegmentID:      m.Int(1, 0),
		SecurityCode:   m.Int64(7, 0),
		BidQty:         m.Int64(2, 0),
		BidPaise:       m.Int64(3, 0),
		AskQty:         m.Int64(5, 0),
		AskPaise:       m.Int64(6, 0),
		LTPPaise:       m.Int64(8, 0),
		LTPQty:         m.Int64(9, 0),
		LastUpdateTime: m.Str(74, ""),
	}
}

// LogonResponse is the decoded form of a 102 Logon Response.
type LogonResponse struct {
	LogonStatus int // tag 70 — 10000 = success
	DaysToExpire int // tag 97 — populated when status=10004 (about to expire)
	SegmentID   int // tag 1 — segments the user is allowed on
}

func (m FixMessage) AsLogonResponse() *LogonResponse {
	if m.Code() != 102 {
		return nil
	}
	return &LogonResponse{
		LogonStatus:  m.Int(70, 0),
		DaysToExpire: m.Int(97, 0),
		SegmentID:    m.Int(1, 0),
	}
}
