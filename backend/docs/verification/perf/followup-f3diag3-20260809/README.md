# q1perf-f3diag3-0809122638-2569920 Core Evidence

This is the committed core subset of the non-baseline-eligible F3/InvalidOrder diagnostic run.

- Backend under test: `260babcd93cfb6a4a58a481cc3c7f8c1f95e455a`
- H5 under test: `df379b5fcc5eb710fc1efa5406c752ba18c3ab30`
- Q1/load tool: `3b5af2eb0758c708edd3a4293093030be9076f94`
- F3: `F3_CHAIN_PASS`
- Independent blocker: `PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG`
- Final verdict: `FAIL_CORRECTNESS`
- Performance baseline: `NOT_RECORDED`
- 30M DAU claim: none

See `../q1-f3-invalid-order-followup-20260809.md` for the analysis. `evidence/pre-overlay-contract-conflict/` retains the smaller first-diagnostic proof of `CONTRACT_CONFLICT / H5_WIRE_THEFT_MISSING`. `evidence-index.json` freezes every retained file hash. Large videos, browser traces, screenshots, and binaries are not committed.
