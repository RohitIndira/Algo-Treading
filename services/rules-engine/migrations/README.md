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
