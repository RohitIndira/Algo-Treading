// Package rulesengine exists to make the module root a valid Go package.
//
// Without at least one .go file at module root, `go test ./...` fails with:
//
//	"no Go files in .../services/rules-engine"
//
// All actual implementation code lives under cmd/ and internal/.
package rulesengine
