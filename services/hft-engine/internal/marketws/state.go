// Tiny helper types used by OdinClient. Kept in their own file so the
// main client.go reads as the protocol logic, not bookkeeping.
package marketws

import (
	"sync/atomic"
	"time"
)

// State is the connection state machine the rest of the engine reads.
type State int32

const (
	StateDisconnected State = iota
	StateConnecting
	StateConnected
)

func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	default:
		return "unknown"
	}
}

// atomicState wraps atomic.Int32 with State semantics. Cheap reads on
// the hot path (manager checks "is feed alive?" on every Entry).
type atomicState struct{ v atomic.Int32 }

func (a *atomicState) Load() State          { return State(a.v.Load()) }
func (a *atomicState) Store(s State)        { a.v.Store(int32(s)) }
func (a *atomicState) CompareAndSwap(o, n State) bool {
	return a.v.CompareAndSwap(int32(o), int32(n))
}

// atomicTime carries the last-tick timestamp with lock-free reads.
// Zero value (.IsZero()) means "no tick yet".
type atomicTime struct{ v atomic.Value }

func (a *atomicTime) Load() time.Time {
	if v := a.v.Load(); v != nil {
		return v.(time.Time)
	}
	return time.Time{}
}

func (a *atomicTime) Store(t time.Time) { a.v.Store(t) }
