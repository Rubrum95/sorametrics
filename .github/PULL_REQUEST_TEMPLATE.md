## What

<!-- One paragraph: the behaviour that changes and why. -->

## Verification

- [ ] `make ci` passes (fmt, check, clippy `-D warnings`, tests)
- [ ] `.sqlx/` regenerated when a query or migration changed
- [ ] Contract change: `scripts/parity_check.py --only <route>` output attached, or the deviation
      is documented in the route's module docs
- [ ] Docs updated (README / docs/operations.md / CHANGELOG)
