# Manthan infra & server migration kit

Declarative data infrastructure for the Manthan stack plus the scripted
migration path from the current manthan-prod (13.206.136.158) to a fresh
server. Full phased runbook (scope table, dependency map, cutover gates):
the "Manthan Prod Migration" artifact, prepared 2026-08-26.

## Contents

| File | Runs on | Purpose |
|---|---|---|
| `docker-compose.yml` | new box | Postgres :5442, Kafka :9192 (+ZK), Redis :6389, Kafka-UI :8083 — faithful to the prod containers, but bound to 127.0.0.1 |
| `init/10-init_databases.sh` | (compose) | creates the canonical DBs on first boot |
| `migrate/00-new-server-setup.sh` | new box | provision: docker, nginx, PM2, Go, repo clone, build all 9 binaries, infra up |
| `migrate/01-quiesce-check.sh` | old box | cutover gate: producers stopped, inbox drained, zero in-flight orders, zero consumer lag |
| `migrate/02-dump.sh` | old box | timestamped bundle: 6 pg dumps + Redis RDB + row-count manifest + checksums |
| `migrate/03-restore.sh` | new box | restore bundle, verify every table count against the manifest |

## Cutover order (Saturday)

```
OLD  pm2 stop data-ingestion rules-engine
OLD  ./01-quiesce-check.sh            # must print QUIESCED
OLD  pm2 stop all
OLD  ./02-dump.sh                     # then scp bundle to new box
NEW  ./03-restore.sh ~/manthan-migration-<ts>   # must print RESTORE VERIFIED
NEW  pm2 start ecosystem.config.js && pm2 save  # watch recovery + reconciler + safety monitor
DNS  Cloudflare A record manthan.stockk.trade -> new IP
OLD  pm2 save                          # saved state = stopped; reboot cannot revive a 2nd stack
```

## Non-negotiables

- **Never both stacks live at once** — two reconcilers on one broker book place
  duplicate orders. DNS is the only switch.
- **Kafka data does not migrate.** After a verified drain there is nothing
  unconsumed; topics auto-create. That is exactly why `01-quiesce-check.sh`
  must pass before `02-dump.sh` (2026-07-17: a half-drained move replayed
  stale SELLs into positions_db).
- **The local Redis DOES migrate.** It holds durable `manthan:*` state —
  daily publish-once signal locks, position/portfolio caches — not just cache.
- **Secrets move by scp only**: `ENCRYPTION_KEY` (broker credentials are
  AES-GCM under it), `INDIRA_API_KEY`, `EXT_REDIS_*`, `LIVEALGOS_LTP_REDIS_*`.
  `EXT_REDIS_ADDR` missing at start silently disables the pipeline (2026-07-16).
- **Confirm broker IP whitelisting with Indira before cutover.**
