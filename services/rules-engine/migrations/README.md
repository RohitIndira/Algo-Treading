# Rules Engine — Database Migrations

Rules-engine is the **lifecycle owner** of the Manthan tables. These migrations
create + evolve that schema. They are service-owned per the bounded-context
ownership rule in [`docs/architecture/data-ownership.md`](../../../docs/architecture/data-ownership.md).

## Target database

| Phase | Database | Status |
|-------|----------|--------|
| Today (dev + staging + prod) | `trading_db` | LIVE |
| After Phase 3 DB redesign cutover | `stockk_trading` | planned — see [`docs/architecture/db-migration-runbook.md`](../../../docs/architecture/db-migration-runbook.md) |

Phase 3 is a `pg_dump`+`psql` copy of the table data into the new domain DB.
The migrations in this directory are NOT re-applied during cutover — the
schema gets pulled forward by the data copy. They're rerun only on a fresh
DB setup (new dev box, new staging cluster, etc.).

## What lives here

| # | File | What it does | Tables touched |
|---|------|--------------|----------------|
| 002 | `002_manthan_portfolio.sql` | Create core Manthan tables | `manthan_positions`, `manthan_portfolio_state`, `manthan_cooldown` |
| 003 | `003_signal_decisions_and_position_events.sql` | CQRS write path | `manthan_signal_decisions` (decisions log) + `manthan_position_events` (event sourcing) + adds columns to `manthan_positions` |
| 004 | `004_backfill_decisions_for_legacy_positions.sql` | One-time data backfill | inserts PROPOSED rows for positions that pre-dated the decisions table |
| 005 | `005_manual_interference_event_types.sql` | Detect manual interference | new event_type values + user_override columns on decisions |
| 006 | `006_active_position_classification_check.sql` | Data integrity | CHECK constraint preventing invalid `status` transitions on `manthan_positions` |
| 007 | `007_manthan_protective_audit.sql` | Protective-attempt audit trail | adds 3 columns to `manthan_positions` for protective-order forensics |
| 008 | `008_drop_trade_signals_table.sql` | Cleanup of dead news-path artefact | DROPs `trade_signals` (was created by the removed migration 001; the code that wrote to it was deleted in commit 671f970) |
| 009 | `009_signal_types_and_outbox_columns.sql` | Extend signal_decisions for all 5 signal types (ENTRY_BUY / SL_MODIFY / EXIT_TSL / EXIT_MANUAL / SL_CANCEL) — additive: `signal_type`, `parent_signal_id`, `payload` columns + CHECK constraints. Per [`docs/rules_engine_refactor.md`](../../../docs/rules_engine_refactor.md) §4.5. | `manthan_signal_decisions` |
| 010 | `010_scope_msd_uniqueness_to_entries.sql` | Scope `uq_msd_per_attempt` UNIQUE to `signal_type='ENTRY_BUY'` — was blocking SL_MODIFY / EXIT_TSL from being inserted at the same second as the entry. | `manthan_signal_decisions` |
| 011 | `011_drop_manthan_position_events.sql` | Drop the event-sourcing table (positions svc owns events now). | `manthan_position_events` |
| 012 | `012_add_kafka_payload_outbox.sql` | Store the exact trade-signals bytes for verbatim recovery re-publish. | `manthan_signal_decisions` |
| 013 | `013_pending_entry_lifecycle.sql` | Confirmation-driven position lifecycle (`PENDING_ENTRY` / `EXPIRED`). | `manthan_positions` |
| 014 | `014_flexi_audit.sql` | Flexi-caps audit: admit `signal_type='FLEXI_EVAL'` + `status='EVALUATED'`, add `flexi_grant JSONB`, partial index. Additive. Split into transactions: tx1 = pre-flight guard (refuses to NARROW a constraint that admits an out-of-band value) + `DROP/ADD … NOT VALID` + `ADD COLUMN` (brief ACCESS EXCLUSIVE); then `VALIDATE` ×2 each in its own tx (SHARE UPDATE EXCLUSIVE — reads/writes continue); then `CREATE INDEX IF NOT EXISTS` (SHARE, sub-second). Target DB **trading_db** (prod: docker Postgres, host port 5442). Apply OFF-HOURS with `-v ON_ERROR_STOP=1` before setting `MANTHAN_FLEXI_CAPS_MODE` (rules-engine forces the mode off at boot if this is missing). FLEXI_EVAL rows are first-write-wins per (strategy, symbol, run_date). See "Flexi caps enable procedure" below. | `manthan_signal_decisions` |

**Migration 001 (`001_create_trade_signals_table.sql`) was removed on 2026-06-25**
when the news-event path it supported was deleted. Production DBs that still
have the `trade_signals` table get it dropped by 008.

## How to apply

There is **no migration runner** wired into the service today (`golang-migrate`
/ `goose` / `atlas` are not used). Migrations are applied manually with `psql`:

```bash
# Local dev
cd ~/Algo-Treading
for f in services/rules-engine/migrations/*.sql; do
  echo "→ $f"
  PGPASSWORD=postgres psql -h localhost -U postgres -d trading_db -f "$f"
done

# Staging / prod — replace creds + DB host
for f in services/rules-engine/migrations/*.sql; do
  psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f "$f"
done
```

All scripts are idempotent (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ...
ADD COLUMN IF NOT EXISTS`, etc.) so re-running is safe.

## Flexi caps enable procedure (migration 014 → `MANTHAN_FLEXI_CAPS_MODE`)

The feature is env-gated and defaults OFF (unset/`off` is byte-identical to
today). First production target is **dry_run for S4450 only**. Order matters:

1. **Off-hours** (outside 09:00–15:30 IST and the 16:35 EOD window), on the
   prod box, against `trading_db` (docker Postgres, host port **5442**):
   ```bash
   psql -h localhost -p 5442 -U $POSTGRES_USER -d trading_db -v ON_ERROR_STOP=1 \
     -f services/rules-engine/migrations/014_flexi_audit.sql
   ```
   If the pre-flight guard RAISEs ("refusing to narrow the constraint"), the
   live CHECK admits a value this repo does not know about — add it to the
   list in 014 and re-run; nothing was changed.
2. **Verify** (both rows must be `convalidated = t`):
   ```sql
   SELECT conname, convalidated, pg_get_constraintdef(oid)
   FROM pg_constraint
   WHERE conrelid = 'manthan_signal_decisions'::regclass
     AND conname IN ('chk_msd_signal_type','chk_msd_status');
   ```
3. Add to the rules-engine `.env` (the PM2 env block in
   `deployments/separate-namespace/ecosystem.config.js` forwards all nine
   `MANTHAN_FLEXI_*` keys as `ENV.<KEY> || ''`):
   ```
   MANTHAN_FLEXI_CAPS_MODE=dry_run
   MANTHAN_FLEXI_STRATEGY_ALLOWLIST=a6cb5b08-dddd-4e54-ad2d-d5ae55edb3c9
   ```
4. **Restart with `--update-env`.** PM2 caches the env at the last
   `pm2 start/restart <file>`; a plain `pm2 restart rules-engine` (and the
   weekday 03:30 `cron_restart`) reuse the OLD env, so the boot log would just
   say "flexi caps off" — easy to misread as a code problem. (rules-engine's own
   `godotenv.Overload(<exe-dir>/../.env)` happens to pick the keys up anyway,
   but do not rely on that.)
   ```bash
   pm2 restart deployments/separate-namespace/ecosystem.config.js --only rules-engine --update-env
   # or: pm2 delete rules-engine && pm2 start deployments/separate-namespace/ecosystem.config.js --only rules-engine
   ```
5. **Confirm in the boot log**: `Manthan flexi caps ENABLED` with
   `mode=dry_run` and the allowlist. `MANTHAN_FLEXI_CAPS_MODE forced OFF —
   audit schema not migrated` means step 1 did not land on this DB; `…
   schema probe failed (DB error …)` means the DB was unreachable at boot
   (retried 3× with 2 s backoff) — fix connectivity and restart.
6. On a dry-run day, "Flexi plan not applied (nothing to lend)" Info lines
   (one per strategy/reason per 5 min) are the positive signal that the
   evaluation IS running even when zero FLEXI_EVAL rows are written.

The **staging** box (43.204.225.116 — runs real funded accounts, never
auto-deploy) uses an untracked server-local ecosystem file that does NOT
forward these keys; add the nine `MANTHAN_FLEXI_*` lines there first if a
dry-run is ever wanted on it.

`MANTHAN_FLEXI_CAPS_MODE=on` with an EMPTY allowlist is fleet-wide on the
first flip; rules-engine logs an Error at boot in that case (it is not forced
off — spec §2.6 defines empty = all). Keep the allowlist set until fleet-wide
is explicitly intended.

## Who owns which table (Phase 5 grants summary)

Per [`scripts/db/phase5_roles_local.sql`](../../../scripts/db/phase5_roles_local.sql)
— after the Phase 5 role grants are deployed:

| Table | Writer(s) | Notes |
|-------|-----------|-------|
| `manthan_positions` | `rules_engine_svc` only | sole lifecycle writer |
| `manthan_position_events` | `rules_engine_svc` only | event-source append-only |
| `manthan_portfolio_state` | `rules_engine_svc` only | capital snapshot |
| `manthan_cooldown` | `rules_engine_svc` only | reentry blocking |
| `manthan_signal_decisions` | `rules_engine_svc` (RW lifecycle) + `rebalancer_svc` (INSERT-only) | co-write boundary, enforced at SQL layer |

## Follow-ups (not in scope here)

- **Adopt a migration runner** for ALL services (golang-migrate or goose).
  Affects rules-engine + user-config + data-ingestion + trade-execution +
  hft-engine. Needs its own design + rollout plan.
- **Phase 3 cutover**: the data copy step in `db-migration-runbook.md`
  pulls these tables into `stockk_trading`. No re-application of these
  migrations is needed during cutover.
