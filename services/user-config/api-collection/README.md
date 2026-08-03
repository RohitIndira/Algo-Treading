# User-Config API Collection

Complete request collection for the user-config service, exercised **through
api-gateway** (`/api/v1`). Three interchangeable formats + one runnable script.

| File | Tool | Use |
|------|------|-----|
| `user-config.postman_collection.json` | Postman / Insomnia / Bruno (import) | GUI testing; Create auto-saves `strategy_id` for the chained requests |
| `user-config.http` | VS Code **REST Client** / JetBrains HTTP Client | Click-to-send inside the editor |
| `run_all.sh` | bash + curl | One-shot CLI smoke test with PASS/FAIL per check |

## Endpoints

All strategy routes are auth-protected. Every request needs:
`Authorization: Bearer <jwt>` **plus** `userId`, `appId`, `source` headers.

| # | Operation | Method | Path |
|---|-----------|--------|------|
| 1 | Create | POST | `/strategies` |
| 2 | Read one | GET | `/strategies/{id}?user_id=` |
| 3 | List | GET | `/users/{user_id}/strategies` |
| 4 | Update | PUT | `/strategies/{id}` |
| 5 | Activate | POST | `/strategies/{id}/activate` |
| 6 | Deactivate | POST | `/strategies/{id}/deactivate` |
| 7 | Delete | DELETE | `/strategies/{id}` |
| 8 | Update credentials | POST | `/auth/credentials` |

## Variables to set

| var | example | notes |
|-----|---------|-------|
| `base` | `http://localhost:8080/api/v1` | api-gateway address |
| `jwt` | `eyJ...` | fresh broker JWT (`loginSource=SSO` → use `source=WEB`) |
| `user_id` | `S4450` | |
| `app_id` | `d0c468...` | **must match the `appId` claim inside the JWT** |
| `source` | `WEB` | SSO tokens → WEB; mobile tokens → IOS/AND |

## Quick start (CLI)

```bash
export JWT="<paste fresh broker JWT>"
# APP_ID auto-derived from the JWT if python3 is present; else export it:
# export APP_ID="<appId claim>"
cd services/user-config/api-collection
chmod +x run_all.sh
./run_all.sh
```

Expected tail:
```
== 13 passed, 0 failed ==
```

## Gotchas (learned the hard way)

- **`$UID` is a bash builtin** (`1000`), not your user id. Use `$USER_ID`.
  A mismatch between the `userId` header and body `user_id` returns **403** (IDOR guard).
- **Read requires `?user_id=`** as a query param — the header alone gives `400`.
- **Update `version`** must equal the strategy's current version (optimistic lock).
  Wrong version → **412**; it increments on every successful write.
- **Delete is a soft STOP** — it sets `stopped_at` (not `deleted_at`) so the row
  **stays GET-able as history**. A read after delete returns **200** with
  `active:false`, *not* 404. (See `migrations/015_add_stopped_at.sql`.)
- **404 test must use a valid non-nil UUID.** The all-zeros UUID equals
  `uuid.Nil` and trips a **400** validation before the existence check.
- **`SQUARE_OFF_AT_MARKET`** on Deactivate/Delete places **real exit orders** —
  never use it while smoke-testing a funded account. Default is
  `KEEP_POSITIONS_OPEN`.
- **`trading_mode:LIVE` + `activate_immediately:true`** arms a real strategy.
  The collection defaults to `PAPER`/`false`.

## Expected response codes

| Scenario | Code |
|----------|------|
| Any valid CRUD op | 200 |
| Empty name / under-capital / nil-UUID | 400 |
| Body `user_id` ≠ header `userId` | 403 |
| Update non-existent (valid UUID) | 404 |
| Update with stale `version` | 412 |

## Where credentials land

`POST /auth/credentials` encrypts (AES-GCM) and upserts
`execution_db.user_credentials.indira_bearer_token`. Verify it's ciphertext,
not the raw JWT:

```bash
PGPASSWORD=postgres psql -h localhost -U postgres -d execution_db -c \
  "SELECT user_id, length(indira_bearer_token) AS len, left(indira_bearer_token,10) AS head \
   FROM user_credentials WHERE user_id='S4450';"
```
`head` should be gibberish (e.g. `5jfSK86HoK`), **not** `eyJ...`.

---

_Verified end-to-end 2026-07-31: full CRUD, version optimistic-lock increments,
400/403/412 error mapping, credentials encrypted at rest._
