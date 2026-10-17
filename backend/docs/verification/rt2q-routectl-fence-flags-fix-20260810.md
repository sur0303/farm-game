# RT2Q routectl fence-flags binding fix (2026-08-10)

## Defect

`cmd/routectl/main.go:registerFenceFlags` returned `fenceFlags` by value after
registering `flag.FlagSet` bindings against the local struct. `Parse` mutated the
original storage while callers validated the returned copy, which stayed zero.

Effect: every live `move-shard` / `takeover-after-expiry` path failed closed at
CLI validation (`errUsage`, empty stdout, exit 1) before the RoutePlan RPC ran.
Unit tests that only asserted non-nil error hid the miss.

## Fix

Return `*fenceFlags` so flag mutations and post-parse validation share one
storage location. Focused tests prove harness-shaped move args bind the expected
fence into `MoveShardRequest`.

## Relation to HandshakeComplete fix

Independent of the VerifyConnection/`HandshakeComplete` correction. Status/shard
reads already worked after that fix; mutation CLIs still failed until this binding
bug was removed.
