# Algo-Treading

Event-driven algorithmic trading platform for Indian equity markets (NSE cash
segment), built as Go microservices around Kafka, PostgreSQL and Redis, executing
through the Indira Securities broker APIs.

The flagship strategy is **Manthan** — a positional 52-week-breakout system
(DELIVERY/CNC, long-only) with a trailing protective-stop framework, submitted
for NSE algo approval. See `docs/Manthan-Strategy-Writeup-v1.0.pdf` and
`docs/architecture/` for the auditor checklists and submission sources.

## Services

| Service | Role |
|---|---|
| `api-gateway` | Public REST API (strategies, live-algos catalog & deployment pages, WS fan-out); auth middleware; talks gRPC to user-config |
| `user-config` | Strategy CRUD + lifecycle events, broker credentials (AES-GCM encrypted at rest), gRPC server |
| `data-ingestion` | Research-sheet ingestion, eligibility screens, idempotent signal publication onto Kafka |
| `rules-engine` | Per-subscription allocation: signal → concentration caps (25% sector / 50% mcap) → capital sizing → entry order publication |
| `trade-execution` | Order lifecycle vs broker: pre-trade checks, price-protected entries, SL place/modify/trail, EOD AMO staging, reconciler, safety monitor |
| `positions` | Confirmation-driven position book (PENDING_ENTRY → ACTIVE → EXIT_PENDING → EXITED) fed by order events |
| `orderstatus` | Order-status projection & reconciliation vs broker order book |
| `portfolio` | Holdings/portfolio views |
| `risk-management` | Pre/post-trade risk counters |
| `rebalancer` | Portfolio rebalancing experiments |

Shared code lives in `pkg/` (notably `pkg/indira` — the broker client — and
`pkg/crypto` for credential encryption). Proto definitions in `api/proto/`.

## Data stores

Six canonical PostgreSQL databases (ownership map: `docs/db_ownership.md`):
`trading_db` (user-config), `signals_db` (data-ingestion/rules-engine),
`execution_db` (trade-execution, incl. `manthan_orders`), `positions_db`,
`order_status_db`, `stockk_market`. Kafka carries signals and order events;
Redis serves live LTP cache and WS pub/sub.

Key invariant: **the position book is confirmation-driven** — only broker-confirmed
fills mutate it, and broker truth wins over intent (reconciler syncs fills,
cancellations and AMO conversions on a fixed cadence).

## Manthan execution safeguards

- Entries: marketable LIMIT at LTP + tick buffer, price-band (circuit) hold,
  margin pre-check, per-(strategy, security, day) idempotency, capped price-chase
  escalation then market fallback.
- Stops: hard 20% protective stop, trailing in 2% steps, never widened. A stop
  outside the day's DPR band is **deferred, never clamped**; software supervision
  covers the gap. Overnight continuity via EOD AMO staging with conversion
  reconciliation (AMO gets a fresh broker id at ~08:50).
- A modify hitting "order not found" at the broker re-arms a **fresh stop** —
  it never touches the position itself (2026-08-26 fix; regression-tested in
  `sl_vanished_order_test.go`). Emergency market sells are latched one-per-symbol
  per window.
- `MANTHAN_MOCK_SESSION=1` bypasses only weekend/holiday gates for exchange
  mock sessions; all other pre-checks stay active.

## Local development

- Postgres runs on the host (`localhost:5432`); Kafka in Docker
  (`localhost:9092`, container `trading-kafka`).
- Build everything: `go build ./...` — tests: `go test ./...`
  (the `trade-execution/internal/manthan` and `api-gateway` suites are the
  most safety-critical).
- Dev/probe tools live under each service's `cmd/` (e.g.
  `trade-execution/cmd/mock-drill-order` for direct-API exchange-limit probes —
  test instruments, never part of the algo path).

## Deployment (manthan-prod)

Production runs on the manthan-prod server under PM2: binaries in
`deployments/separate-namespace/bin/`, one process per service, env supplied at
first start (plain `pm2 restart <svc>` preserves it — **trade-execution requires
`EXT_REDIS_ADDR` at every restart** or the pipeline silently disables).

Deploy discipline:
1. Build from a tree at **verified file parity** with the server
   (`md5sum` compare — see `docs/architecture/` runbooks); the repo on `dev`
   is the source of truth.
2. Keep a dated `.bak-YYYYMMDD` of the previous binary next to the new one.
3. Restart trade-execution outside market hours unless fixing an active incident.
4. Postgres for the algo stack is the `algo-dev-postgres` container
   (host port 5442).

## Documentation

- `docs/db_ownership.md` — database ownership source of truth
- `docs/architecture/` — design docs, NSE auditor-checklist sources and PDF
  build scripts (`build_uat_pdf.py`, `build_mock_pdf.py`, `build_writeup_pdf.py`)
- `docs/audit/` — session evidence for the exchange mock (2026-08-22) and UAT
  (2026-08-24/25) runs
- Service-level design notes: `docs/*.md`
