# Friend hot fixture + Share Due filter Premerge Gate

## Identity

- Integration base: `be41b4ea911ec8705e10c787f78f48829cbc49d7`
- Reviewed head: `c02675c9f497dd287d5f6a3a96c65bff412271aa`
- Premerge merge head: `d34aad82d59a66e4a8e7864469db26fdc5268173`

## Gate

```text
go test ./... -count=1                                  PASS
go vet ./...                                            PASS
go build ./cmd/...                                      PASS
focused Friend/CAP Race                                 PASS
scope allowlist                                         PASS
git diff --check                                        PASS
real Redis Share same-Bucket Due filter ×20            PASS
focused Quota/Share matrix                              4800/4800/0 PASS
```

No Redis container/process remains and no credential is retained.

Correctness history remains unchanged:

```text
failed Warmup 045335 = FAIL_CORRECTNESS
600 original Quota operations = INSUFFICIENT_EVIDENCE
Share setup stop 050113 = retained
```

Premerge authorizes integration only. Product code and the CAP binary changed, so the previous backend image and Manifest binding cannot authorize a new Smoke/Formal run.

```text
premerge_gate = PASS
formal_authorized = false
baseline_eligible = false
capacity_claim = NONE
```
