# SORA Mainnet Runtime Metadata

`sora-mainnet.scale` is a pinned snapshot of the SORA Substrate runtime
metadata, used by `subxt` codegen at compile time
(`#[subxt::subxt(runtime_metadata_path = "metadata/sora-mainnet.scale")]`).

Pinning the file (vs fetching at build time) gives us:

- Reproducible builds with no network dependency.
- Compile-time errors on chain upgrades that break our event decoders.
- Smaller compile surface — we restrict the metadata to the pallets we
  actually decode events from.

## Pallets included

| Pallet | Reason |
|--------|--------|
| `System` | Always required by subxt (extrinsic success/fail, block hashes). |
| `Timestamp` | `set` extrinsic carries the block timestamp. |
| `Assets` | `Transfer` events. |
| `LiquidityProxy` | DEX `Exchange` events (swaps). |
| `XorFee` | Fee burn events. |
| `SubstrateBridgeApp` | Hashi v2 substrate-side bridge events. |
| `ParachainBridgeApp` | Hashi v2 parachain bridge events (XCM). |
| `JettonApp` | Hashi v2 TON bridge events. |
| `BridgeMultisig` | Hashi v1 (legacy) bridge multisig events. |

When Phase 1.2 expands to other event types, regenerate with the new pallet list.

## Regenerating

```bash
# Requires: cargo install subxt-cli  (matching the subxt crate version)

cd crates/ingest/metadata

subxt metadata \
  --url wss://ws.mof.sora.org \
  --pallets "System,Timestamp,Assets,LiquidityProxy,XorFee,SubstrateBridgeApp,ParachainBridgeApp,JettonApp,BridgeMultisig" \
  --format bytes \
  > sora-mainnet.scale
```

Then run `cargo build` — the subxt macro will pick up the new file.

## Drift detection

`subxt` will fail the build if the metadata changes shape in a way that
breaks our decoders. To explicitly verify a running node still matches
the pinned metadata:

```bash
subxt compatibility \
  --url wss://ws.mof.sora.org \
  --metadata sora-mainnet.scale \
  --pallets-only
```

This is wired into Phase 1.2's CI as a non-blocking check.
