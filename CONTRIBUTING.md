# Contributing

## Branches

`v33` is the integration branch of the rewrite; production still runs the Node service from
`main`. Feature work goes on branches off `v33` and lands through a pull request.

## Commits

Conventional Commits, scope = crate or area: `api: …`, `ingest: …`, `db: …`, `ops: …`,
`docs: …`. The subject states the behaviour change; the body states what was verified and how.

## Before opening a pull request

```bash
make ci        # fmt --check, cargo check, clippy -D warnings, cargo test
```

- Every SQL statement is `sqlx::query!` / `query_as!`; regenerate `.sqlx/` with
  `cargo sqlx prepare --workspace` and commit it.
- No `unwrap()` in non-test code; errors carry context.
- No silent fallbacks: a value that cannot be produced is an error or a documented `None`.
- A route or decoder is done when `scripts/parity_check.py` passes for it against production, or
  when the deviation is documented in the module docs.
- Migrations are additive and idempotent; never edit an applied migration.

## Pull request checklist

The template in `.github/PULL_REQUEST_TEMPLATE.md` asks for: tests, clippy, fmt, `.sqlx`
regenerated when queries changed, parity evidence for contract changes, docs updated.
