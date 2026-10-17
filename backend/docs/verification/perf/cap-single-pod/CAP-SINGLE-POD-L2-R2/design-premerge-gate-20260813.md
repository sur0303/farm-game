# CAP-SINGLE-POD-L2-R2 Design Premerge Gate

- Design base: `6f239b136e0943ec0dc30e3c92644974fc2b3454`
- Reviewed design head: none — the design set is uncommitted working-tree state on the base above
- Review result: `design-review-result-20260813.md` (authoring review, P0=1 P1=4 P2=1)
- Premerge candidate: not yet committed

R1 could name a reviewed design head and a premerge candidate commit. R2 cannot yet, and the two identifiers are recorded as absent rather than filled in. What binds the content until they exist is `design-index.json`, which pins the eight contract outputs by byte length and exact-file SHA-256.

## Gate results

```text
verify-contract on this directory           PASS   L2_CONTRACT_VERIFY=PASS
exact design hash index (8/8)               PASS
go build -buildvcs=false ./cmd/cap-single-pod  PASS
go vet -buildvcs=false ./cmd/cap-single-pod    PASS
go test -buildvcs=false -count=1 ./cmd/cap-single-pod/  PASS
JSON parse of every JSON file added         PASS
collector template renders                  PASS   render-run-plan.py
binding trust root re-derived from disk     PASS   f4d6910e509cecbba76de45d5b928b0b731f57cfecf07c8d8ad8ec2602a77db3
scope review                                PASS
secret review                               PASS
```

`-buildvcs=false` is required on every Go command here. The checkout's `.git` is a worktree pointer, so the toolchain reports `error obtaining VCS status: exit status 128` for each main package. That is a property of the checkout, not of the code.

## What was not run, and why

```text
go test ./...                               NOT RUN
go vet ./...                                NOT RUN
```

The working tree carries concurrent edits from other trains under `internal/` and `cmd/cap-correctness/`. A repository-wide result would report their state, not this change, so it is left unrun rather than reported as a pass that does not belong to this gate.

## Scope

Changed here: `docs/verification/perf/cap-single-pod/CAP-SINGLE-POD-L2-R2/**`, `deploy/load/cap-single-pod-r2-collector-adapter.template.json`, and two files in the harness — `cmd/cap-single-pod/required_test.go` gains a test that the R2 collector binding names exactly one producer for `support_service_primary_queue_items`, and `cmd/cap-single-pod/binding.go` was re-checked against the binding on disk and needed no edit. All of that is inside the Train A write scope. No production semantics, image, fixture or frozen identity value changed.

The secret review covers the one endpoint added: `http://127.0.0.1:19082/metrics`, loopback, credential-free, no query and no fragment, which is what `validateLoopbackHTTP` in the adapter enforces anyway.

## Result

```text
PREMERGE_GATE=PASS_WITH_FINDINGS_OPEN
```

The gate passes on the mechanical checks it owns. It authorises nothing beyond the paperwork: Observability Qualification, Load Generator Qualification, Smoke and every pressure phase stay `NOT_RUN`, Mixed stays blocked, and `P0-CONTRACT-IDENTITY-01` — `contract.md` freezing an identity the binding does not — sits in front of all of them.
