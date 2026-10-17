# RT2Y Route Controller Plan TTL Handoff

- task: RT2Q-discovered route-controller Plan TTL composition blocker and exact-review correction
- frozen pushed base: `edd53ed6ad14256c994371be845a2da05de24672`
- first signed code commit: `eb765cdc9187d5ea17fd14c62038392a97ca1a30`
- exact reviewed head: `683df6286a016cf823b0d44aebf3a9a6595ec5fc`
- signed corrective code commit: `9c03ea4401ab307d1fa500149081cbd983181e9c`
- Writer: same sole production composition Writer, OpenAI Sol/xhigh
- nested agents: none
- push: none
- status: Writer correction implemented and verified; no post-fix independent review closure is claimed; RT2Q Final runtime evidence remains pending

## Corrected finding

The exact review of `683df628...` found that the first RT2Y change configured only
`PlanExecutorOptions.PlanTTL`. Production still constructed the Redis PlanStore from its default
`24h` `Options.PlanTTL`, so an Executor configured above `24h` could start but its initial CAS was
rejected by the store's logical-lifetime check.

The correction remains limited to the original route-controller configuration/composition scope:

- production derives Redis routeplan options from the existing `DefaultOptions(namespace)`, then
  assigns the same immutable `cfg.planTTL` used by the Executor before constructing the PlanStore;
- no second parser or Plan TTL default exists: the existing strict environment/flag parser,
  `24h` default, `7d` maximum, and shared timing validator remain the only command-root policy;
- both production construction helpers reuse the shared timing validator, so above-`7d`,
  operation-timeout-plus-safety, and millisecond-precision unsafe states fail startup/construction;
- all other Redis PlanStore options and behavior remain sourced from `DefaultOptions`, including
  independent audit retention; no Redis PlanStore implementation or storage contract changed;
- the deployment documentation now states that one setting configures both the Executor horizon and
  Redis PlanStore lifetime limit while audit retention remains unchanged.

No `internal/routeplan`, Redis PlanStore implementation, audit-retention policy, Proto/generated,
dependency, Authority, Manager, Holder, routectl, consumer-routing, Compose-value, leader/lease, or
revision-state-machine surface changed in the corrective commit.

## Deterministic coverage

The focused production-construction integration regression uses the real Redis PlanStore against
miniredis and covers `120s`, `25h`, and exact `7d` configurations. For each horizon it proves:

- the created Executor retains the exact configured Plan TTL and operation timeout;
- the production-configured PlanStore loads its scripts and accepts an initial Plan whose logical
  lifetime is exactly the configured horizon;
- the same PlanStore rejects an otherwise valid initial Plan at horizon plus `1ms`, proving the
  store did not retain a wider or narrower default;
- the physical Redis TTL remains logical horizon plus the unchanged PlanStore default audit
  retention.

A second construction regression proves above-`7d`, operation-safety mismatch, and the unsafe
sub-millisecond budget fail both command startup validation and the production construction helpers.
The existing strict parser regression continues to prove that `7d+1ms` is rejected from environment
and CLI inputs.

## Verification

Passed on the exact code content in `9c03ea4401ab307d1fa500149081cbd983181e9c`:

- the six exact Plan TTL parser/construction/Compose regressions at `-count=100` and
  `-race -count=100`;
- all `cmd/route-controller` tests at `-count=1`;
- `go test -buildvcs=false -count=1 -timeout=600s ./...`;
- `go test -buildvcs=false -race -count=1 -timeout=900s ./...` on the second run;
- `go vet ./...`;
- `go build -buildvcs=false ./cmd/...`;
- `./scripts/gen-proto.sh`, followed by zero diff under `api/proto` and `api/gen`;
- real base-plus-remote Compose rendering with `ROUTE_CONTROLLER_PLAN_TTL=25h`; the focused Compose
  regression also repeatedly covered unset, explicit-empty, and `120s` rendering;
- `git diff --check`, exact original-authorized-path/frozen-surface checks, and an added-diff secret
  scan;
- cryptographic SSH `git verify-commit` success for the signed corrective code commit.

The first full race run encountered the unrelated existing
`internal/friend.TestConcurrentAddBothDirectionsAndDelete` active-slot concurrency failure. The
corrective diff contains no `internal/friend` or dependency change; no out-of-scope workaround was
made. The second complete race run passed.

## Remaining gate

This handoff records same-Writer corrective implementation and verification only. It does not claim
an independent post-fix `NO FINDINGS` review or review closure. RT2Q Final must still execute and
independently attest its multi-Pod planned Drain, controller restart, commit-unknown recovery, crash
takeover, Fresh Join, gameplay, cleanup, and four-truth reconciliation against the final candidate.
Nothing here claims RT2Q acceptance, Premerge, G1/G2/G3, CP, HA, leader election, or autoscaling
readiness.
