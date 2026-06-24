# Data Ownership Matrix

> The single source of truth for "which service owns which table."
> If a table isn't listed here, it shouldn't exist.

> **For HOW services talk to each other, see [communication-patterns.md](communication-patterns.md).**

## The five rules

1. **One writer per table.** Exactly one service is listed as Owner.
2. **Other services read via the owner's gRPC API** when business logic is required (decryption, computed values, audit logging).
3. **Direct DB read is allowed** only for owned data + api-gateway public list views with a SELECT-only Postgres role. **Backend services default to gRPC** when reading cross-service data — see Rule 4 of [communication-patterns.md](communication-patterns.md) for the full decision matrix.
4. **Every new table requires an entry here** in the SAME PR that creates the migration. CI enforces this.
5. **Postgres roles enforce the boundary.** Code bugs cannot write to tables they shouldn't, because Postgres denies the GRANT.

### Quick decision tree for "where should this read live?"

```
Is the read from api-gateway?
├── YES → Direct DB ✅ (api-gateway is the translation layer)
└── NO  → Is decryption / computed value / audit / filter needed?
         ├── YES → gRPC (mandatory)
         └── NO  → Does the read meet all four:
                   (1) raw, (2) owner agrees, (3) SELECT-only role,
                   (4) hot path with read replica?
                   ├── YES → Direct DB acceptable (rare)
                   └── NO  → gRPC (default)
```

The most common honest answer for backend services is **gRPC**. If you're tempted to justify a direct cross-service read, double-check against [communication-patterns.md#rule-4](communication-patterns.md) before merging.

---

## The "Rule of 3 Domains" — why we have 3 DBs (not 7, not 1)

Every trading platform on earth has roughly **three irreducible data domains**:

```
1. AUTH/IDENTITY  → who are you, what can you access  (security boundary)
2. TRADING        → what did you buy, sell, hold, owe  (business core)
3. MARKET DATA    → what's the world doing             (read-heavy time-series)
```

These domains have **fundamentally different** profiles:

|                    | Auth                  | Trading              | Market data           |
|--------------------|-----------------------|----------------------|-----------------------|
| Sensitivity        | Highest (PII, JWTs)   | High (orders, P&L)   | Low (public market)   |
| Write rate         | ~1/sec                | ~100/sec             | ~10,000+/sec          |
| Read profile       | small lookups         | mixed                | analytical / scans    |
| Compliance         | GDPR + KYC + 7-yr     | SEBI 7-yr retention  | None                  |
| DB technology      | Postgres OK           | Postgres             | TimescaleDB           |
| Failure tolerance  | Down = can't login    | Down = can't trade   | Down = stale prices   |

Mixing these domains in one DB creates problems:
- Backup strategies must satisfy the strictest domain (auth)
- High-volume market data writes can starve auth queries
- A schema change to market data shouldn't require auth team approval

**Our three databases:**

```
stockk_auth      ← auth/identity domain    (owner: user-config)
stockk_trading   ← trading business domain (shared by 4 services)
stockk_market    ← market data domain      (owner: data-ingestion, TimescaleDB)
```

### Why this scales — "the test"

> Can we add 50 new tables over the next 5 years WITHOUT redesigning?

**Yes**, because every conceivable new table fits one of the three domains:

| New table you might add        | Goes into        | Why                       |
|--------------------------------|------------------|---------------------------|
| `users`, `sessions`            | stockk_auth      | identity                  |
| `subscription_plans`           | stockk_auth      | user-owned config         |
| `kyc_documents`                | stockk_auth      | PII, compliance boundary  |
| `user_preferences`             | stockk_auth      | user-scoped               |
| `audit_log`                    | stockk_auth      | who-did-what tracking     |
| `broker_charges_master`        | stockk_trading   | cost data for orders      |
| `pnl_snapshots`                | stockk_trading   | trading domain            |
| `algo_backtest_results`        | stockk_trading   | strategy outputs          |
| `option_greeks`                | stockk_trading   | derived from positions    |
| `fii_dii_data`                 | stockk_market    | market-wide stats         |
| `options_chain`                | stockk_market    | market data               |
| `corporate_actions`            | stockk_market    | symbol-level events       |
| `nse_holidays`                 | stockk_market    | market reference data     |

A 4th DB would only ever be justified if we entered a totally new domain:

- `stockk_internal` — admin tools / employee dashboards
- `stockk_compliance` — regulatory submissions (separate retention rules)
- `stockk_partners` — multi-tenant B2B partner data

**Until then: 3 DBs.**

---

## The CQRS read/write split — when gRPC is REQUIRED vs when direct DB is OK

CQRS = **Command Query Responsibility Segregation**. Different rules for writes vs reads.

### Writes (commands) — STRICT, always via owner

```
ANY service wanting to WRITE to a table → use the owner's gRPC API.
NEVER write to another service's table directly.
```

**Why:**
- Owner validates input
- Owner enforces invariants (e.g., "qty must be > 0")
- Owner audit-logs the write
- Owner can change schema without breaking writers (gRPC has versioning)
- Owner can rate-limit / throttle / queue writes

### Reads (queries) — PRAGMATIC, gRPC only when needed

A read can use direct DB IF AND ONLY IF all four apply:

1. ✅ You're reading from a table inside YOUR bounded context (same DB)
2. ✅ No decryption is needed
3. ✅ No business logic / derived values needed
4. ✅ No audit logging required for this read

If ANY of those is NO → use gRPC.

### gRPC is REQUIRED for reads when:

| Scenario                          | Why gRPC                          | Example                       |
|-----------------------------------|-----------------------------------|-------------------------------|
| Decryption needed                 | Only owner has the key            | user_credentials.bearer_token |
| Audit logging required (compliance) | Owner logs every access          | user data for SEBI audit      |
| Derived/computed value            | Owner has the computation         | net P&L, available margin     |
| Cache layer in front of DB        | Owner manages the cache           | Strategy metadata             |
| Rate-limited resource             | Owner enforces limits             | Risk metric queries           |
| Returning subset of fields        | Owner decides what's safe to expose | User profile (no SSN)       |

### Direct DB read is OK when:

| Scenario                          | Why direct is fine                | Example                       |
|-----------------------------------|-----------------------------------|-------------------------------|
| api-gateway serving public lists  | Pure data fetch, low latency      | GET /orders for mobile        |
| Service reading its own tables    | Single-writer = self-read OK      | trade-exec reading manthan_orders |
| Read replica for analytics        | Offload load from primary         | Reporting dashboards          |
| Cross-table joins in same DB      | gRPC can't join across services   | Orders JOIN positions JOIN strategies |

---

## Postgres role enforcement (the GUARANTEE)

Even with good rules, code has bugs. Postgres roles enforce the boundary so that **even buggy code cannot violate ownership**.

### Pattern: one role per service, minimal grants

```sql
-- ─────────────────────────────────────────────────────────────
-- Service-specific Postgres roles (run once during DB setup)
-- ─────────────────────────────────────────────────────────────

-- user-config: owns stockk_auth
CREATE ROLE user_config_svc LOGIN PASSWORD 'from_vault';
ALTER ROLE user_config_svc CONNECTION LIMIT 25;
ALTER ROLE user_config_svc SET statement_timeout = '10s';
GRANT CONNECT ON DATABASE stockk_auth TO user_config_svc;
GRANT USAGE ON SCHEMA public TO user_config_svc;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO user_config_svc;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO user_config_svc;

-- trade-execution: writes most of stockk_trading
CREATE ROLE trade_execution_svc LOGIN PASSWORD 'from_vault';
ALTER ROLE trade_execution_svc CONNECTION LIMIT 30;
ALTER ROLE trade_execution_svc SET statement_timeout = '10s';
GRANT CONNECT ON DATABASE stockk_trading TO trade_execution_svc;
GRANT USAGE ON SCHEMA public TO trade_execution_svc;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    orders, manthan_orders, manthan_order_events, manthan_arm_retries,
    signal_inbox, execution_events, execution_outbox,
    manthan_positions, manthan_position_events
TO trade_execution_svc;
-- read-only on strategies (owned by rules-engine)
GRANT SELECT ON strategies, strategy_conditions, trade_configs, risk_limits TO trade_execution_svc;

-- api-gateway: read-only on stockk_trading for list views (CQRS read path)
CREATE ROLE api_gateway_reader LOGIN PASSWORD 'from_vault';
ALTER ROLE api_gateway_reader CONNECTION LIMIT 50;
ALTER ROLE api_gateway_reader SET statement_timeout = '5s';
GRANT CONNECT ON DATABASE stockk_trading TO api_gateway_reader;
GRANT USAGE ON SCHEMA public TO api_gateway_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_gateway_reader;
-- EXPLICITLY no writes, even by accident
REVOKE INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public FROM api_gateway_reader;

-- Future tables auto-grant
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO api_gateway_reader;
```

### Now even if api-gateway has a bug:

```go
// api-gateway code with a hypothetical bug:
db.Exec("DELETE FROM manthan_orders WHERE id = $1", orderID)
// ↓
// Postgres response: ERROR: permission denied for table manthan_orders
```

**Postgres prevented the disaster.** Code review is the first line, roles are the safety net.

---

## Current ownership matrix (target state after DB redesign)

### `stockk_auth`

| Table                | Owner        | gRPC required? | Direct read by                  |
|----------------------|--------------|----------------|---------------------------------|
| user_credentials     | user-config  | **YES (decryption + audit)** | none (always via gRPC) |
| users *(future)*     | user-config  | YES (PII filter)             | none                   |
| sessions *(future)*  | user-config  | YES (auth check)             | none                   |
| audit_log *(future)* | user-config  | YES                          | none                   |

### `stockk_trading`

| Table                      | Owner            | gRPC required? | Direct read by         |
|----------------------------|------------------|----------------|------------------------|
| orders                     | trade-execution  | No             | api-gateway (read-only) |
| manthan_orders             | trade-execution  | No             | api-gateway, rebalancer (read-only) |
| manthan_order_events       | trade-execution  | No             | api-gateway, rebalancer (read-only) |
| manthan_arm_retries        | trade-execution  | No             | none                   |
| signal_inbox               | trade-execution  | No             | none                   |
| execution_events           | trade-execution  | No             | api-gateway, rules-engine (read-only) |
| execution_outbox           | trade-execution  | No             | kafka publisher (internal) |
| hft_audit_orders           | hft-engine       | No             | api-gateway (read-only) |
| hft_runtime_state          | hft-engine       | No             | api-gateway (read-only) |
| strategies                 | rules-engine     | YES (active set, filter logic) | api-gateway (read-only) |
| strategy_conditions        | rules-engine     | No             | trade-execution, rebalancer (read-only) |
| trade_configs              | rules-engine     | No             | trade-execution (read-only) |
| risk_limits                | rules-engine     | YES (cached, computed) | trade-execution (read-only) |
| trade_signals              | rules-engine     | No             | trade-execution (read-only) |
| manthan_positions          | trade-execution  | No             | api-gateway, rebalancer, rules-engine (read-only) |
| manthan_position_events    | trade-execution  | No             | rules-engine (read-only) |
| manthan_portfolio_state    | rebalancer       | YES (computed P&L) | api-gateway (read-only) |
| manthan_signal_decisions ⚠ | rules-engine *   | No             | rebalancer (co-INSERT), api-gateway (read-only) |
| manthan_cooldown           | rules-engine     | No             | rebalancer (read-only) |

### `stockk_market`

| Table              | Owner          | gRPC required? | Direct read by                   |
|--------------------|----------------|----------------|----------------------------------|
| tick_data          | data-ingestion | No             | rules-engine, hft-engine, trade-execution (read replica) |
| daily_ohlcv_*      | data-ingestion | No             | rules-engine, rebalancer        |
| instruments        | data-ingestion | YES (resolve symbol) | rules-engine               |
| manthan_stocks     | data-ingestion | No             | rules-engine                    |
| manthan_signals    | data-ingestion | No             | rebalancer                      |
| breakout_events    | data-ingestion | No             | rules-engine                    |

#### ⚠ Co-INSERT pattern — `manthan_signal_decisions`

This is the one table in our codebase with a **documented multi-writer**
pattern. It is NOT a bug.

  - **Lifecycle owner**: rules-engine (writes UPDATEs for the state
    machine: PROPOSED → DISPATCHED → CONFIRMED / FILLED / REJECTED).
    All UPDATE paths live in
    `services/rules-engine/internal/manthan/{publisher,position_projector,fill_consumer}.go`.

  - **Co-inserter**: rebalancer (writes ONLY the initial INSERT with
    `status='PROPOSED'`, never UPDATE). Code at
    `services/rebalancer/internal/publisher.go:EnsureDecisionRow`.
    Triggered by the batch CLI / top-up path.

  - **Why it's safe**:
      - Both INSERTs use `ON CONFLICT (signal_id) DO NOTHING` — signal_id
        is unique, so a race produces exactly one row, not two.
      - Both INSERTs write identical column set + the same `'PROPOSED'`
        status. rebalancer additionally sets `rejection_reason` for
        top-ups (a nullable string column rules-engine ignores).
      - rules-engine guards all UPDATEs on the current state (e.g.,
        `WHERE status='PROPOSED'`), so a late dispatch can't regress a
        row that's already CONFIRMED.

  - **Cleanup path** (future): when rules-engine has a gRPC server
    (Phase 1.1), rebalancer can call a `RecordDecision` RPC instead of
    INSERTing directly. Until then, the dual-writer pattern is the
    pragmatic choice — the duplication is ~15 lines of identical SQL,
    extracted to `pkg/manthan/decisions/insert.go` would be a low-value
    refactor.

  - **What this means for Phase 2 (DB redesign)**: this table moves with
    rules-engine into `stockk_trading`. rebalancer needs a Postgres role
    with INSERT-only grant on this specific table (not full write
    access), enforcing the lifecycle boundary even if a future bug
    tried to UPDATE from rebalancer.

---

## Current migration state — what's in flight (2026-06-24)

The Phase 0 / Phase 1 / Phase 2 plan in
[rules-engine-split-design.md](rules-engine-split-design.md) is being executed
incrementally. This section is the running snapshot of where things stand
so a new reader does NOT assume the target-state matrix above is reality
today.

**Fully migrated to gRPC (target state achieved):**

  - rules-engine → user-config strategies (Phase 0.1, commit 4cdcc75)
  - rebalancer → user-config strategies (Phase 0.2, commit fc88bee)
  - rebalancer → user-config user_credentials (Phase 0.6a, commit 7d33090)
  - trade-execution → user-config user_credentials (Phase -1, commit 38631ee;
    has a DB fallback documented as the hot-path safety net)

**Acceptable as direct DB during migration (per CQRS policy, Rule 4 of
[communication-patterns.md](communication-patterns.md))**:

These are backend-to-backend reads of raw data with NO decryption / NO
derived values / NO audit need. They satisfy the "four conditions" rule
and may stay as direct reads provided the reader has a SELECT-only
Postgres role (Phase 2 / 4 will enforce this at the DB layer):

  - rebalancer → rules-engine.{manthan_positions, manthan_cooldown,
    manthan_portfolio_state, manthan_signal_decisions}
  - rules-engine → data-ingestion.manthan_signals
  - All api-gateway cross-service reads (api-gateway exception)

Default for any NEW backend-to-backend read going forward: **gRPC**.
Direct DB requires explicit justification under Rule 4's four conditions
+ a row added to this file's ownership matrix.

**❄ Frozen pending later decision — hft-engine**

hft-engine currently reads `user_credentials` (with client-side decryption,
violating the single-owner-of-secrets principle) and `strategies` (with
business logic — strategy_type filter, JOIN with trade_configs). Both are
KNOWN debt:

  - `services/hft-engine/internal/repo/repo.go:74`   — strategies SQL JOIN
  - `services/hft-engine/internal/repo/repo.go:143`  — user_credentials read

Project decision (2026-06-24): hft-engine is FROZEN for the current
migration sweep. These two direct reads remain as-is. They WILL be
addressed in a later phase. Any code review that adds NEW direct reads
in hft-engine should still reject — the freeze is "don't ship new fixes,"
not "anything goes." The freeze unblocks Phase 2 (DB layout / Postgres
roles) — hft-engine simply gets the same per-service Postgres role with
its current grants, and the role's grants are tightened once the
migration ships.

---

## Anti-patterns we're fixing (the original sins)

| Symptom seen today                         | Why it's wrong                     | Fix                                   |
|--------------------------------------------|------------------------------------|---------------------------------------|
| 4 services write `user_credentials`        | Multiple writers, schema chaos     | user-config is single writer; others use gRPC |
| `execution_outbox` in user-config's DB     | Wrong owner — events are from trade-exec | Move to stockk_trading, owner trade-exec |
| 3 copies of `health_probes`                | Each DB has its own                | Drop; use Grafana liveness probe       |
| `manthan_signals` in market_data, orders in trading_execution | Can't transact across | Both in stockk_trading after redesign |
| Hardcoded `postgres` superuser in all .env | One credential = one breach loses all | Per-service roles with minimal grants |
| No `statement_timeout`                     | One slow query blocks everyone     | Set per-role limits |

---

## The future-table playbook — adding a new table without redesigning

When you need to add a new table, follow this checklist:

### Step 1: Pick the domain (which DB)

```
Question: "Who CONCEPTUALLY owns this data?"

The user / their identity / their config         → stockk_auth
A trade, order, position, strategy, or rule       → stockk_trading
Market price, instrument, signal, or analytics    → stockk_market
```

If you can't decide, it usually means the table is doing TWO things and should be split into two tables.

### Step 2: Pick the owner service

```
Question: "Who writes to this table?"

That service is the owner. Add it to the ownership matrix in this file.
```

### Step 3: Decide on read paths

For each other service that needs this data:
- Does it need decryption / computation / audit? → require gRPC
- Is it just reading? → direct read with SELECT grant
- Is it a Kafka consumer reacting to events? → owner publishes events, consumer reads from Kafka

### Step 4: Write the migration

```bash
# In the owner service's migrations directory
touch services/<owner>/migrations/NNN_create_<table>.sql
```

Migration must be:
- Additive (no destructive changes)
- Reversible (have a `_down.sql` if needed)
- Include `COMMENT ON TABLE ... IS 'OWNER: <service>. ...';`

### Step 5: Update this file (data-ownership.md)

Add a row to the appropriate domain table with:
- Table name
- Owner
- gRPC required? Yes/No (with reason if yes)
- Direct readers list

### Step 6: PR checklist

The PR description must include:
- [ ] Domain chosen (auth / trading / market) and rationale
- [ ] Owner service identified
- [ ] Migration file in `services/<owner>/migrations/`
- [ ] Entry added to `data-ownership.md`
- [ ] gRPC RPC defined (if owner-only reads required)
- [ ] Postgres GRANTs added to deployment script (if non-owner reads allowed)

### Step 7: Code review

The PR requires:
- Owner team approval (data definition)
- Architecture review IF: new gRPC RPC, new direct readers, new bounded context

---

## How to DEPRECATE a table

1. Stop new writes in code (deploy & verify zero writes for 1 week)
2. Stop new reads in code (deploy & verify zero reads for 1 week)
3. Migration to `DROP TABLE` — must include archive step (export to S3 first)
4. Remove entry from this file in the same PR
5. After 30 days, drop the Postgres role grants that mentioned the table

---

## See also

- [communication-patterns.md](communication-patterns.md) — the 4 patterns (gRPC, Kafka, direct DB, HTTP)
- [database-redesign-plan.md](database-redesign-plan.md) — the migration plan from current to target state
