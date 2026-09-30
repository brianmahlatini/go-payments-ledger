# go-payments-ledger

An event-sourced, double-entry payments ledger in Go, written with the **standard library only** (zero third-party dependencies).

It covers the parts of a money service that break in production: double-posting under retries, races between concurrent transfers, deadlocks, crash consistency and silent tampering with history.

## Guarantees

| Property | How it is enforced |
|---|---|
| No floating-point money | Every amount is `int64` minor units (cents). JSON floats are rejected at decode time. |
| Double entry | Every transfer is one event with a debit leg and a credit leg. The trial balance per currency is always exactly `0`, and a test asserts it under concurrent load. |
| No double-posting | `Idempotency-Key` is mandatory on `POST /v1/transfers`. A replay returns the original transfer (`Idempotent-Replayed: true`). The same key with a different payload returns `422`. Concurrent duplicates block on the first attempt instead of racing. Keys are stored in the journal, so they survive restarts. |
| No lost updates | Each account has its own mutex. A transfer locks both accounts in ascending ID order, so opposing transfers (A→B while B→A) cannot deadlock. |
| Crash consistency | Write-ahead: the event is appended and `fsync`ed before in-memory balances change. On startup the journal is replayed. A torn final line from a crash mid-write is truncated because it was never acknowledged. Corruption anywhere else stops startup: dropping acknowledged money movements is never acceptable. |
| Tamper evidence | Events are SHA-256 hash-chained (`hash = sha256(prev_hash ‖ event)`). `GET /v1/admin/verify` recomputes the chain. |
| No overdrafts | Customer accounts cannot go negative. Only accounts opened with `allow_overdraft` (funding or settlement accounts) can. Overflow of `int64` is checked explicitly. |

## Architecture

```
cmd/ledgerd          process wiring: config, journal replay, graceful shutdown (SIGTERM drain)
internal/ledger      domain: accounts, transfers, idempotency, hash-chained journal, replay
internal/httpapi     JSON/HTTP: strict decoding, RFC 9457 problem responses, request IDs, logging
```

**Lock order** is always *account locks (sorted by ID)* → *journal lock*. `TrialBalance` takes every account lock in the same global order to get a consistent snapshot without stopping the world.

## API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/v1/accounts` | `{"name","currency","allow_overdraft"}` → `201` + `Location` |
| `GET` | `/v1/accounts/{id}` | balance, version, `ETag` |
| `GET` | `/v1/accounts/{id}/statement?after=&limit=` | keyset pagination with a running balance per line |
| `POST` | `/v1/transfers` | requires `Idempotency-Key`. `201` when new, `200` on replay |
| `GET` | `/v1/admin/trial-balance` | bearer `ADMIN_TOKEN`, constant-time compare |
| `GET` | `/v1/admin/verify` | recomputes the hash chain |

Errors use `application/problem+json`. Internal errors are logged with the request ID and never leaked to the client.

```bash
LEDGER_JOURNAL_PATH=./ledger.jsonl ADMIN_TOKEN=dev go run ./cmd/ledgerd

curl -XPOST localhost:8080/v1/accounts -d '{"name":"funding","currency":"ZAR","allow_overdraft":true}'
curl -XPOST localhost:8080/v1/accounts -d '{"name":"alice","currency":"ZAR"}'
curl -XPOST localhost:8080/v1/transfers -H 'Idempotency-Key: pay-001' \
  -d '{"from_account_id":"acc_…","to_account_id":"acc_…","amount_minor":15000,"currency":"ZAR"}'
```

## Tests

```bash
make race    # go test -race -count=1 ./...
make bench
```

The tests cover the following:
- 16 goroutines × 500 random transfers, including opposing pairs, with a 20-second deadlock watchdog. Money is conserved and no customer account goes negative.
- 50 concurrent requests sharing one idempotency key produce exactly one posting.
- A failed attempt (insufficient funds) is not cached, so it can be retried once the account is funded.
- A journal crash with a torn write is recovered on restart, and idempotency still holds afterwards.
- Editing an event in history is detected by chain verification.
- HTTP: unknown fields, float amounts, trailing JSON, a missing idempotency key and admin authentication.

CI runs gofmt, `go vet`, staticcheck, race tests with coverage, govulncheck and a Docker build. The image is distroless and runs as non-root.

## Trade-offs and what I'd do next

- **One `fsync` per transfer** is the safest default, but it caps throughput at the disk's sync rate. The next step is group commit: batch several events per fsync while holding acknowledgements.
- **Single node.** Balances live in memory and the journal is the source of truth. Scaling out means sharding by account, with cross-shard transfers going through a saga (reserve → post → release). An alternative is to move the journal to Postgres with `SELECT … FOR UPDATE` in sorted ID order, which uses the same ordering idea as here.
- **Failed attempts are not cached.** This is a deliberate choice. Stripe-style APIs cache 4xx responses too. Here a client can retry after fixing the cause.
