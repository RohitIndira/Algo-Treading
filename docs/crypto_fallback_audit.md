# `pkg/crypto.Decrypt` plaintext-fallback audit

**Status:** investigation for a follow-up PR. No code change to `pkg/crypto` is made in
this PR — this document is the starting point for the migration + hard-fail change.

## Background — what the fallback does

`pkg/crypto/crypto.go` `Decrypt()` has three "return the input unchanged" fallback
branches instead of returning an error:

| Line | Condition | Current behaviour |
|------|-----------|-------------------|
| `crypto.go:57-59` | input is not valid base64 | returns the raw input string |
| `crypto.go:72-75` | ciphertext shorter than the GCM nonce | returns the raw input string |
| `crypto.go:80-82` | `gcm.Open` fails (bad MAC / **wrong key** / corruption) | returns the raw input string |

The intent was to tolerate legacy **unencrypted** tokens that predate encryption. The
side effect is that **a wrong `ENCRYPTION_KEY` is indistinguishable from success**: a
caller that rotated the key incorrectly gets back the base64 ciphertext as if it were a
JWT, with no error. This is why `T30` ("wrong key → decrypt fails, does NOT return
plaintext or panic") currently fails.

---

## 1. AUDIT — are any `user_credentials` rows plaintext today?

The real column is `indira_bearer_token` (the audit query in the task used the
placeholder name `encrypted_token`). Run per environment:

```sql
SELECT user_id,
       length(indira_bearer_token)          AS len,
       substring(indira_bearer_token,1,12)  AS head,
       (indira_bearer_token LIKE 'eyJ%')    AS looks_like_plaintext_jwt
FROM user_credentials
ORDER BY updated_at DESC;
```

A plaintext JWT starts with `eyJ` (base64 of `{"`). AES-GCM ciphertext (base64 of
`nonce||ciphertext`) has a random-looking head and never starts with `eyJ`.

### Result — LOCAL `execution_db` (2026-07-30)

| user_id | len | head | looks_like_plaintext_jwt |
|---------|-----|------|--------------------------|
| S4450 | 464 | `gHjlffk7UiUb` | false |
| ISPL19122 | 476 | `p3lzLdqLia+t` | false |
| ND03920 | 472 | `zwnR8cTe37QI` | false |

All three rows are ciphertext. **No plaintext rows locally.**

> **Not yet run on staging/prod.** The staging server (43.204.225.116) holds real funded
> accounts and was not touched by this audit. Ops must run the query above on
> staging and prod `execution_db` before the hard-fail change and paste results here. If
> any row shows `looks_like_plaintext_jwt = true`, that row must be encrypted first
> (step 4a).

---

## 2. CALLERS of `crypto.Decrypt`

`grep -rn "crypto.Decrypt" services/ pkg/` (excluding tests):

| Caller | What it does with the return value | Breaks if `Decrypt` returns an error instead of fallback? |
|--------|------------------------------------|-----------------------------------------------------------|
| `services/user-config/internal/repository/credentials_repository.go:101` | `bearerToken, err := crypto.Decrypt(...)`; on `err != nil` returns `fmt.Errorf("decryption error…")`. On success returns the token in `IndiraCredentials`. | **No.** Already checks and propagates `err`. A hard-fail becomes a clean `INTERNAL` at the gRPC layer instead of a silently-wrong token. |
| `services/trade-execution/internal/repository/credentials_repository.go:77` | `decryptedToken, err := crypto.Decrypt(...)`; on `err != nil` returns `err`. On success returns the token. | **No.** Already checks and propagates `err`. |

There are only two `Decrypt` call sites. Both already handle a non-nil error correctly,
so the hard-fail change is **source-compatible** — no caller code needs editing.

`Encrypt` call sites (for completeness — unaffected by the change):
`user-config/.../credentials_repository.go:62`, `trade-execution/.../credentials_repository.go:48`.

> Note: in the gRPC deployment (`USER_CONFIG_GRPC_ADDR` set), trade-execution does **not**
> decrypt locally — it calls user-config's `GetUserCredentials` RPC and receives an
> already-decrypted token, falling back to its own DB repo (which decrypts) only on RPC
> error. So user-config is the primary decrypt path; trade-execution's is a fallback.

---

## 3. IMPACT — what breaks if we hard-fail today?

- **Accounts relying on the fallback:** none found locally (all 3 rows encrypt/decrypt
  cleanly with the configured key). Unknown for staging/prod until §1 is re-run there.
- **Failure mode of a wrong-key or corrupt row after hard-fail:**
  - user-config `GetUserCredentials` → `INTERNAL` gRPC error → trade-execution's
    `grpc_credentials_repository` treats it as an RPC error and falls back to the local
    DB repo, which would also hard-fail → the user's order path sees
    `ErrCredentialsNotFound`/decrypt error → **auth-expired / order-rejected**, surfaced
    to the UI as a re-login prompt. No crash loop (errors are returned, not `panic`).
  - This is strictly better than today, where a wrong key returns garbage that gets sent
    to the broker as a bearer token and fails auth **without any signal that the key is
    wrong** (looks like an expired session instead of a config error).
- **Who is affected right now:** with the correct `ENCRYPTION_KEY` set, nobody — the
  fallback is dormant. The risk is silent: it only bites during a key rotation or a
  partial-encryption migration, exactly when you most want a loud failure.

---

## 4. MIGRATION PLAN (follow-up PR)

Ordered, idempotent, one environment at a time (local → staging → prod):

**a. Encrypt any legacy plaintext rows in-place.** One-shot script that, for each
`user_credentials` row where `indira_bearer_token LIKE 'eyJ%'` (or otherwise fails a
decrypt round-trip), runs `crypto.Encrypt(token, ENCRYPTION_KEY)` and writes it back.
Idempotent: skip rows that already decrypt cleanly. Run once per env.

**b. Verify.** Re-run the §1 query — every row must show `looks_like_plaintext_jwt =
false`. Additionally run a decrypt round-trip check (small Go tool) asserting every row
decrypts to a token that parses as a JWT with the env's key.

**c. Change `pkg/crypto.Decrypt` to hard-fail on GCM auth error.** Remove the three
fallback branches (`crypto.go:57-59`, `72-75`, `80-82`); return `err` (and a distinct
`ErrDecryptFailed`) instead of the input string. Keep the empty-string short-circuit
(`crypto.go:46-48`) since an empty token is a legitimate "no creds" sentinel.

**d. Coordinated deploy.** `pkg/crypto` is shared, so ship together:
`user-config`, `trade-execution`, and any other module importing `pkg/crypto`
(`orderstatus`, `rules-engine`, `rebalancer` — re-grep at PR time; today only
user-config and trade-execution call `Decrypt`). Deploy after (a)+(b) confirm every env
is fully encrypted, so the newly-strict `Decrypt` never meets a legacy plaintext row.

**Pre-req gate:** do **not** merge (c) until (a)+(b) are green on staging **and** prod.
