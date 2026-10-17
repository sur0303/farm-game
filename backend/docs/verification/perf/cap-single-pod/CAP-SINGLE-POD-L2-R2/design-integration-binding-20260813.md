# CAP-SINGLE-POD-L2-R2 Design Integration Binding

- Design base: `6f239b136e0943ec0dc30e3c92644974fc2b3454`
- Frozen R2 runtime evidence head: `6f239b136e0943ec0dc30e3c92644974fc2b3454`
- Frozen R2 business commit: `5ce43715363eea4f18e0925b6992429785053261`
- Active binding: `deploy/load/cap-single-pod-r2-binding.json`, SHA-256 `f4d6910e509cecbba76de45d5b928b0b731f57cfecf07c8d8ad8ec2602a77db3`
- Design premerge: not yet committed
- Design integration merge: not yet committed

R1 bound its integration to a premerge commit and a merge commit. R2 cannot yet: the design set described here is working-tree state on top of the design base, and the two commit identifiers do not exist. They are recorded as absent rather than filled with a plausible value; whoever commits this set writes them in, and the exact-file index in `design-index.json` is what binds the content until then.

The design base and the frozen runtime evidence head are the same commit. That is a property of R2, not a coincidence to be relied on: the capacity identity `dyn-friend-r72-capacity-20260813` was built from this head, so the documents and the measured image share one base.

## What this integration changes

Documentation under `docs/verification/perf/cap-single-pod/CAP-SINGLE-POD-L2-R2/**`, and one test-only adapter template under `deploy/load/`. Both are inside the Train A write scope that the R1 design review authorised. No production semantics, no image, no fixture and no frozen identity value changed. The binding file itself is unchanged, so its digest and the trust root compiled into `cmd/cap-single-pod/binding.go` still agree with the file on disk.

## What this integration does not change

The frozen identity stays exactly as the binding states it. `product_acceptance_eligible` stays `false`; the acceptance verdict stays with `dyn-friend-r70-formal-20260813`. Observability Qualification, Load Generator Qualification, Smoke and every pressure phase remain `NOT_RUN`. Mixed remains blocked. `baseline_eligible` stays `false`, `capacity_claim` stays `NONE`, and `supports_30m_dau` stays `false`.

The independent review of this design set has not happened. `design-review-result-20260813.md` is an authoring review and says so, and it leaves one P0 and four P1 findings open.
