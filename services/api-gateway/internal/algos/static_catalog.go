package algos

import "context"

// StaticCatalog is the in-memory implementation of the Catalog interface.
// It holds the algo list as a plain Go slice — no DB, no Redis, no network.
//
// Why start with a static catalog?
//
//	We have exactly ONE algo today (Manthan). Building a database table,
//	migrations, gRPC endpoints, and admin tooling just to store one row
//	would be over-engineering. The static catalog gets us to a working
//	frontend in minutes; when we have 10+ algos and need a non-developer
//	to edit them, we'll swap to a DB-backed catalog. The handler won't
//	notice the change — that's the whole point of the Catalog interface.
//
// Why is it a struct (with no fields) instead of a plain function?
//
//	Two reasons:
//	  1. Symmetry with the future. A *DBCatalog will hold a DB connection;
//	     a *RedisCatalog would hold a Redis client. Wrapping the static
//	     version in a struct keeps every implementation the same shape,
//	     so the wiring code (router) stays uniform.
//	  2. Methods on a struct can be added without breaking callers. If we
//	     later need to add caching, a struct is the right home for it.
type StaticCatalog struct {
	// items is the in-memory list. Built once at startup by NewStaticCatalog
	// and never mutated again — so we can share it across goroutines without
	// a lock. (A future DB-backed catalog would have totally different
	// internals, which is why this field is private — it's an implementation
	// detail, not part of the contract.)
	items []Algo
}

// NewStaticCatalog returns a Catalog populated with the algos we ship by
// default. Today: just Manthan.
//
// Pattern note: we return the interface type (Catalog), not the concrete
// type (*StaticCatalog). That trains the caller to depend on the
// abstraction, not on this specific implementation. Tomorrow we'll do
// the same with NewDBCatalog — same return type, different innards.
func NewStaticCatalog() Catalog {
	return &StaticCatalog{
		items: []Algo{
			manthan(),
		},
	}
}

// All satisfies the Catalog interface by returning every algo we have.
//
// The receiver is `s *StaticCatalog` (pointer). For a read-only method on
// such a small struct, value receivers would work too, but the convention
// in Go is: pick one (pointer or value) and use it for ALL methods on the
// type. Pointer receivers are the safer default — they avoid copying the
// struct on every call and let you add stateful methods later without
// changing the signature.
//
// `ctx` is ignored here because there's no IO to cancel. The signature
// keeps it for future-proofing — see catalog.go.
//
// Errors are always nil. There's no failure mode for reading a Go slice.
// But we still return error in the signature, again because the interface
// promises it, and that promise is what makes swapping in a DB-backed
// catalog later a transparent change.
//
// We return a COPY of the slice (`append([]Algo(nil), s.items...)`)
// instead of the original. If we returned s.items directly, a caller
// could accidentally modify it (e.g. `result[0].Name = "Hacked"`) and
// the change would silently affect every future caller. Copying costs
// almost nothing here (one algo, ~200 bytes) and makes the catalog
// genuinely read-only from the outside.
func (s *StaticCatalog) All(ctx context.Context) ([]Algo, error) {
	_ = ctx
	out := append([]Algo(nil), s.items...)
	return out, nil
}

// manthan is the canonical Manthan algo entry as shown on the Explore
// screen. Kept as a separate function (instead of an inline literal) so:
//   - The constructor's slice initialization stays one line and reads cleanly.
//   - When we add the second algo, both factory functions sit side-by-side.
//   - When this data eventually moves to a DB row, the migration script
//     can mirror this exact shape one-to-one.
//
// Field-by-field this matches the Manthan card on the Explore screen.
func manthan() Algo {
	return Algo{
		ID:          "algo_manthan_v1",
		Name:        "Manthan",
		Type:        "Equity",
		Style:       "Positional",
		Logo:        "https://stockk-assets.s3.ap-south-1.amazonaws.com/algos/manthan.png",
		Description: "Manthan screens the Nifty 500 every Monday using a weighted blend of momentum, quality, low-volatility and value signals.",
		Badge:       "Most subscribed",
		// 15,00,000 rupees = ₹15 Lakhs. We send raw rupees as an int64
		// so the API stays stable when the frontend changes its display
		// format ("15 Lac" / "15 Lakhs" / "₹15,00,000").
		MinInvestment: 500_000,
		// −12.6%. Stored as a plain number; the frontend adds the "%" suffix.
		MaxDrawdown: -12.6,
		// Map keys exactly match what the frontend expects to render.
		// Adding "5Y Return" later means one new key here, nothing else.
		PrimaryReturn: map[string]float64{
			"3Y Return": 28.4,
			"2Y Return": 32.9,
		},
	}
}

// Compile-time assertion: if *StaticCatalog ever stops satisfying the
// Catalog interface (e.g. someone renames All or changes its signature),
// this line will fail to compile, surfacing the mistake at build time
// instead of as a confusing runtime error later.
//
// Read it as: "I claim that nil-of-type-*StaticCatalog is a valid Catalog.
// Compiler, prove me wrong."
var _ Catalog = (*StaticCatalog)(nil)
