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
| `Assets` | Asset registry types (`AssetId32`). |
| `Balances` | `Transfer` events for native XOR (the Node live mechanism). |
| `Tokens` | ORML `Transfer` events for every other asset (the Node live mechanism). |
| `LiquidityProxy` | DEX `Exchange` events (swaps). |
| `XorFee` | Fee burn events. |
| `SubstrateBridgeApp` | Hashi v2 substrate-side bridge events. |
| `ParachainBridgeApp` | Hashi v2 parachain bridge events (XCM). |
| `JettonApp` | Hashi v2 TON bridge events. |
| `BridgeMultisig` | Hashi v1 (legacy) bridge multisig events. |
| `TransactionPayment` | `TransactionFeePaid` events (per-extrinsic network fee). |
| `PoolXYK` | `Reserves` storage for `/pools`. |
| `EthBridge` | Classic Ethereum bridge: `transfer_to_sidechain` call, `RequestRegistered` / `IncomingRequestFinalized` events, `requests` storage. |

When Phase 1.2 expands to other event types, regenerate with the new pallet list.

## Regenerating

```bash
# Requires: cargo install subxt-cli  (matching the subxt crate version)

cd crates/substrate/metadata

subxt metadata \
  --url wss://mof2.sora.org --version 15 \
  --pallets "System,Timestamp,Assets,Balances,Tokens,LiquidityProxy,XorFee,SubstrateBridgeApp,ParachainBridgeApp,JettonApp,BridgeMultisig,EthBridge,TransactionPayment,PoolXYK" \
  --format bytes \
  > sora-mainnet.scale
```

Then run `cargo build` — the subxt macro will pick up the new file.

Notes (2026-08-10, spec 130 refresh):

- `--version 15` is required: the node now serves metadata v16 by default
  and subxt 0.38 only understands up to v15.
- `wss://ws.mof.sora.org` rejects external connections since ~2026-06;
  use `wss://mof2.sora.org` for any probe or regeneration from the Mac.
- `sora-mainnet.scale.bak.spec124` is the previous pin (runtime 4.8.3),
  kept for diffing. The 9-pallet subset compiled unchanged against
  spec 130 — none of our decoded events changed shape in 4.8.5–4.8.8.

## Drift detection

`subxt` will fail the build if the metadata changes shape in a way that
breaks our decoders. To explicitly verify a running node still matches
the pinned metadata:

```bash
subxt compatibility \
  --url wss://mof2.sora.org --version 15 \
  --metadata sora-mainnet.scale \
  --pallets-only
```

This is wired into Phase 1.2's CI as a non-blocking check.
