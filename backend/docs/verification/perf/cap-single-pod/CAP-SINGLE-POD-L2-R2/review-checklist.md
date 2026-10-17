# CAP-SINGLE-POD-L2-R2 Design Review Checklist

R2 binds a capacity identity, not an acceptance identity. The reviewer must review the exact Design Head, decide every item below against the checked-in R2 files and the r75 receipts, and return P0/P1/P2 counts. Any unchecked required item keeps Observability Qualification, Load Generator Qualification and Smoke blocked.

The R1 checklist in `../CAP-SINGLE-POD-L2-R1/review-checklist.md` still governs everything R2 inherits unchanged. This file adds only what R2 changed. It exists so that an R1 answer cannot be reused where R2 has a different fact.

## Identity: which binaries carry the verdict, which carry the load

- [ ] `deploy/load/cap-single-pod-r2-binding.json` and `profile.json` state the same six frozen identity values, and the harness proves it rather than asserting it.
- [ ] The frozen backend evidence head is the r75 capacity head `6f239b136e0943ec0dc30e3c92644974fc2b3454`, not an R1 head and not the r70 acceptance head.
- [ ] `contract.md` freezes the same identity the binding freezes. A contract that names a different identity is a P0, not a wording defect.
- [ ] The reason the frozen identity moved off r72 is a defect in the r72 binaries, stated with the gate it fails, and not a convenience. A capacity number taken on binaries that fail a hard correctness gate is not a capacity number.
- [ ] The r70 acceptance binaries and the r75 capacity binaries are **not** the same binaries, and no R2 file claims they are. The r72 per-binary digest receipt does not carry to r75.
- [ ] Because the binaries differ, a Formal tier requires product acceptance re-obtained on the r75 binaries. Load Generator Qualification does not, because it measures the generator rather than the service, and the reviewer must check that no R2 file lets that exemption widen.
- [ ] The reason the two image digests differ is stated, bounded to the build context, and checked against the source diff.
- [ ] `capacity_identity.eligible_for_product_acceptance` is `false` in the receipt and `product_acceptance_eligible` is `false` in `profile.json`, and no R2 file contradicts either.

## Login posture and configuration deviation

- [ ] The deployment delta between the acceptance identity and the capacity identity is enumerated, not summarised: app profile, injected load-test token, and the opportunity rung the run is born on.
- [ ] Every anti-abuse ceiling raised for the capacity identity is listed with its product value, its capacity value and its scope, and the ceilings left untouched are listed with the reason they must stay binding.
- [ ] The evidence for raising each ceiling is an observation, not an expectation: the rejected-connection arithmetic reproduces from the recorded rate and burst.
- [ ] No R2 document offers a number taken on this identity as a statement about how the product behaves against abusive clients.

## Narrowed required metric set

- [ ] Every metric dropped relative to R1 carries a reason naming the concrete gap, and the reason is checkable against the scrape or the source.
- [ ] Each dropped metric is classified as either exported under another name with no reader, or not exported at all; nothing is dropped as merely inconvenient.
- [ ] The narrowing states the round at which the dropped metrics are restored.
- [ ] The survey the narrowing rests on names the identity it was generated against, and the reviewer has decided whether that identity is close enough to the one that will be measured.
- [ ] The correctness gate list contains only gates the oracle actually measures; gates that are not instrumented are listed separately with what each one would need.
- [ ] Metrics that the harness binary demands regardless of this file are called out as not narrowable, so a reader cannot mistake the narrowed list for the full demand.
- [ ] Substitution and log estimation remain forbidden; a narrowed set is not a licence to approximate what is left.

## Collector mapping

- [ ] Every logical name the harness demands has exactly one producer, and no logical name is mapped twice.
- [ ] For each mapping, the aggregate matches the label cardinality the exporter actually emits; a `SINGLE` aggregate over a labelled family, or a `SUM` over a family with unavailable series, is a defect.
- [ ] Where a chosen source under-covers the quantity its logical name claims, the shortfall is written down next to the mapping.
- [ ] Where a chosen source encodes observation failure as a value rather than as an absence, the encoding and its effect on slope and threshold judgements are written down.
- [ ] Where a rejected alternative would have been the obvious choice, the reason for rejecting it is recorded.
- [ ] The mapping set is frozen by hash before a run consumes it, and the run plan pins the rendered configuration.

## Contract and harness agreement

- [ ] The hard correctness gates named in `contract.md` are a subset of the gates in `required-metrics.json`; a gate the prose demands and the file omits is a contradiction.
- [ ] The design review head compiled into the harness names an R2 review result, or the gap is recorded as blocking an R2 manifest.
- [ ] `manifest.schema.json` constants match the binding's schema versions exactly, and the binding's recorded schema digest matches the file on disk.

## Exact-file index

- [ ] `design-index.json` covers the eight contract outputs, each with byte length and SHA-256 of the exact file.
- [ ] `verify-contract` passes against the checked-in directory with no file edited after the index was written.
- [ ] The index names the design base it was taken against.

## Claim boundary

- [ ] `baseline_eligible`, `capacity_claim` and `supports_30m_dau` are unchanged by anything in R2.
- [ ] No phase beyond design is recorded as anything other than `NOT_RUN` until it has run.
- [ ] Any review recorded in `status.json` states whether it was independent.

## Required reviewer output

```text
P0=<n>
P1=<n>
P2=<n>
IDENTITY_BINDING=PASS|FAIL
PRODUCT_ACCEPTANCE_SEPARATION=PASS|FAIL
CONFIG_DEVIATION_DISCLOSED=PASS|FAIL
METRIC_NARROWING_JUSTIFIED=PASS|FAIL
COLLECTOR_MAPPING=PASS|FAIL
EXACT_FILE_INDEX=PASS|FAIL
OBSERVABILITY_QUALIFICATION=BLOCKED|AUTHORIZED
LOAD_GENERATOR_QUALIFICATION=BLOCKED|AUTHORIZED
SMOKE=BLOCKED|AUTHORIZED
PRODUCT_ACCEPTANCE_CLAIM=NONE
MIXED_B0=BLOCKED
CAPACITY_CLAIM=NONE
```
