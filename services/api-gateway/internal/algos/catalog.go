package algos

import "context"

// Catalog is the "contract" that any algo data source must fulfil.
//
// What is an "interface" in Go?
//
//   An interface is a list of method signatures — no actual code, just a
//   promise. Any type that has methods matching the signatures automatically
//   satisfies the interface. We don't need to write "implements" anywhere
//   (unlike Java/C#); the Go compiler figures it out from the method names.
//
// Why have a Catalog interface at all? Why not just call a function?
//
//   Today we hardcode Manthan into a Go file (StaticCatalog, next file).
//   Tomorrow we'll likely store algos in Postgres. Maybe later we'll fetch
//   metrics from an external "performance" service.
//
//   Without an interface, every change to where data comes from would
//   ripple through every file that touches algos. Handlers would need to
//   change. Tests would need to change. With an interface, ALL OF THAT
//   CODE keeps using `Catalog` — only the implementation behind it changes.
//
//   This is called "depending on abstractions, not concretions" — one of
//   the most important habits in growing a codebase you can refactor
//   without fear.
//
// Why does the method take a ctx and return an error if the static
// implementation will never fail?
//
//   Because the FUTURE database implementation absolutely can fail (DB
//   down, query timeout, network blip) and CAN be cancelled mid-flight
//   (user closed the tab, request timed out). If we wrote `All() []Algo`
//   today, we'd have to refactor every caller in three months when we
//   move to DB. By including ctx + error in the contract NOW, the
//   handler is already future-proof. Today's StaticCatalog just returns
//   nil for the error and ignores ctx — no harm done.
//
//   This is the standard Go idiom: any function that does (or could do)
//   IO takes ctx as its first argument and returns error as its last.
type Catalog interface {
	// All returns every algo available on the Explore screen.
	//
	// ctx — passed down to any IO the implementation does. Cancellation
	//       and deadlines flow through it. The static catalog ignores it;
	//       a DB catalog would pass it to QueryContext.
	//
	// Returns the algos in the order the implementation considers
	// "default" — typically alphabetical or curated. The HTTP handler
	// returns them in that same order; no sorting on top.
	//
	// Returns (nil, err) on a real failure (DB down, network blip).
	// Returns ([]Algo{}, nil) — an EMPTY slice with no error — when
	// there genuinely are no algos to show. Callers should treat the
	// empty list as "show an empty state," not as an error.
	All(ctx context.Context) ([]Algo, error)
}
