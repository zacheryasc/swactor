# swactor

Minimal actor runtime for Rust. One trait, one message type.
Single-threaded (`tick()`) or multi-threaded (`run()`).

## Description

Core runtime is `src/`. Actors implement `ActorInterface` (in `actor.rs`),
interact through `Ctx` (in `runtime.rs`), and run on worker threads (`worker.rs`).

`crates/` builds upward: `std` adds OTP patterns (supervision, monitoring, groups),
`distribution` adds clustering, everything else composes from there.

## Dev commands

`cargo xtask --help` for available test groups.

## Testing

```
cargo check --workspace
cargo xtask test <your-feature-crate>
cargo xtask test essential
```
