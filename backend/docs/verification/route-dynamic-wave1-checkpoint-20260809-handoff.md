# CP-ROUTE-DYNAMIC-WAVE1 — Handoff

Date: 2026-08-09
Branch: `checkpoint/route-dynamic-wave1-20260809`
Frozen code base: `29ed7be2f7ddc6df2e837830c165ad50ca62e28f`
Baseline contract/docs checkpoint: `d4c5c001d8dd785e8dc5b3058e3e45a7f11b50e4`

## Included final heads

- RT2A Snapshot Store/Publisher: `2e8c3ccd7611935657d361b7fd08b9f03b4ddd2f`
- RT2B Dynamic Route Consumer: `6a8b61ca91d5e9ad0cb2305db41eb6f90c7ac1dc`
- RT2Q-0 Evidence Harness: `f99fb566d4d008cef1400a4417ab1108454acfa2`
- Combined feature merge state before this handoff: `768a7244583a15e63d1b2bfef4f3a66f024fbfb4`

Each feature head was implemented and repaired only in its owned worktree by an official OpenAI Codex `gpt-5.6-luna`, reasoning `max`, Writer. Independent exact-head Luna closure reviews reported `NO FINDINGS` and `NO BLOCKING FINDINGS`. I0 pushed the three feature branches only after closure.

## Frozen interfaces and behavior

- Complete durable RouteSnapshot envelope with schema/checksum/topology validation and strict GlobalRevision CAS.
- Store-first durable publisher with replayable notification failure.
- Durable bounded TransitionRecovery/Tombstone adapters; complete atomic Redis physical snapshot validation.
- Pump-only `RemoteAssignmentInstaller` capability, separate from read-only provider and Manager publisher capabilities.
- Initial authenticated full fetch, full-snapshot Watch, reconnect/resync, bounded liveness failures, topology/generation fail-closed, and zero remote calls on Resolve hot paths.
- E1 evidence harness with exact provenance and artifact binding, redaction, exact-key cleanup, Linux descriptor/pidfd capabilities, supervised process cleanup, and fail-closed unsupported platforms. E1 cannot be promoted to E3/E4.

This checkpoint does not contain holder control, route-controller transport, RoutePlan/routectl, production composition, multi-pod evidence, or autoscaling.

## I0 combined gates

Executed on the combined Wave-1 worktree after A → B → Q merge:

```text
go test -buildvcs=false -count=1 -timeout=600s ./...                                      PASS
go test -race -buildvcs=false -count=1 \
  ./internal/routecontrol/... \
  ./internal/storage/redis/routecontrol/... \
  ./internal/actorrouting/... \
  ./internal/testkit/routecontrol/... \
  ./cmd/rt2q-route-control-harness/...                                                    PASS
GOFLAGS=-buildvcs=false go vet ./...                                                       PASS
go build -buildvcs=false ./cmd/...                                                        PASS
GOOS=windows GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol       PASS
GOOS=plan9 GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol         PASS
git diff --check d4c5c001..HEAD                                                           PASS
git status --porcelain                                                                    CLEAN
```

Feature-worktree I0 gates additionally ran RT2A Redis tests at `-count=20`, RT2Q focused tests at `-count=10`, all three owned race suites, full-repository tests, vet/build, and E1/E3 CLI exit checks. All passed.

## Wave-2 entry rule

The final commit containing this handoff is the exact Wave-1 checkpoint base. RT2C must start first from that SHA. Per RC-SCB-1, RT2D cannot start against an invented transport/identity contract; it starts only after RT2C interface closure and a new exact checkpoint. RT2I remains the sole Writer for the four production command roots and `deploy/**`. RT2M may call the Manager/Control Service but may not create a second authority or WriterEpoch state machine.

The integration branch advanced independently to `c28034f5bfef4c21566d71096924c04de11d8e63` while Wave 1 was in progress. Feature branches are intentionally not rebased. Final Route Control Premerge must be created from the then-latest remote integration and merge each frozen feature head exactly once.

Autoscaling remains blocked until G1/G2/G3 and `CP-ROUTE-CONTROL-MVP` are merged into a new integration baseline.
