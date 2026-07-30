# Known issues — user-configuration flow

## OPEN: PAUSED MANTHAN strategies resurrect as active on rules-engine restart

**Severity:** High (a strategy the user paused starts trading again after an
unrelated rules-engine restart).

**Status:** Partially mitigated. The **STOPPED** half is fixed (see below); the
**PAUSED** half remains open because the real fix spans a service boundary that
was out of scope for the user-config-only PR that closed the STOPPED case.

### Mechanism

1. `user-config` `StrategyRepository.ListAllActive` returns MANTHAN rows with
   `(active = true OR strategy_type = 'MANTHAN')`. Because of the `OR`, a
   **paused** MANTHAN strategy (`active = false`, `stopped_at IS NULL`) is still
   returned by the bulk-load query.
2. `rules-engine` bulk-load (`internal/startup/bootstrap.go`
   `validateStrategyConfig`) force-sets `s.Active = true` on every row it
   receives — comment: *"MANTHAN always active once created"*.

Net effect: on a rules-engine restart, a paused MANTHAN strategy is reloaded and
flipped back to active, so it resumes generating signals. The pause only lives in
rules-engine's in-memory `Paused` index, which is lost on restart.

### What was fixed (2026-07-30)

`ListAllActive` now also filters `AND stopped_at IS NULL`, so a **STOPPED**
(terminal) strategy is never bulk-loaded and can no longer resurrect. This closes
the dangerous half — a user who explicitly Stopped a strategy will not have it
come back to life.

### What remains / proposed fix (needs coordinated PR)

The PAUSED case needs a `user-config` + `rules-engine` change together — do not
fix one side alone:

- **Option A (preferred):** `rules-engine` bulk-load respects the incoming
  `Active` flag instead of force-setting `true`, and places `active = false`
  MANTHAN rows into its `Paused` index. `user-config` keeps returning paused rows
  (so rules-engine knows they exist) but tagged with their real `active` state.
- **Option B:** `ListAllActive` returns only `active = true AND stopped_at IS
  NULL`, and rules-engine treats "absent from bulk-load" as paused. Simpler on
  the user-config side, but loses the paused strategy from rules-engine entirely
  until the next explicit RESUME event, so the UI's paused list must be sourced
  from the DB, not rules-engine.

Either way, the fix is cross-service and should not land as a user-config-only
change. Tracked separately from the STOPPED fix.
