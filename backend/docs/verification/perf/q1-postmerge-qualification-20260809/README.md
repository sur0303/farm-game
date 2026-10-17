# CP-Q1-POSTMERGE-QUALIFICATION

Result: `PASS (1/1)`.

## Frozen integration truth

- Backend integration code commit: `4824c9c02fe70873b2f9a8eab3ce274d227063a8`
- H5 integration commit: `525c4d324100b17a891f02797c4880fa9a02f70a`
- Backend image ID: `sha256:f36a4d8f5f8c05ac44ed173895e46f5348af6d5b133835e6d25d13da2b51c39b`
- H5 image ID: `sha256:9bfdedaa70f7cbf8bac86bb25cc2ff62dba1cf7832fb5d8f66c72c1f113486fd`
- Qualification run: `q1perf-postmerge-0809150124`

Canonical multi-stage image builds were attempted first. The Backend container could not resolve the private module host and the H5 isolated builder hit an npm CLI failure. No source fallback or different commit was used: exact integration sources were built on the host, hashed, and copied into equivalent Alpine/nginx runtime images. Build methods and hashes are retained in `evidence-index.json`.

## Qualification gates

- full Backend tests: PASS
- focused race, vet, and all command builds: PASS
- legacy CARE Due compatibility and accepted/rejected corruption controls: PASS
- Harness negative controls and restored clean control: PASS
- H5 204 tests, TypeScript, and web build: PASS
- Browser + Go Bot + Authority + F1/F2/F3: PASS
- `F3_CHAIN_PASS`
- InvalidOrder observations: `0`
- physical/unmatched Pair Due residual: `0/0`
- `CareCompleted=2`
- first/second-click Wire THEFT: `0 → 1`
- hard correctness incidents: `0`

This checkpoint is a postmerge correctness qualification with `baseline_eligible=false`. It records no throughput, P99, single-Pod capacity, or 30-million-DAU conclusion.

Machine-readable identities and evidence hashes: [`evidence-index.json`](evidence-index.json).

Next authorized work package: `CAP-REDIS-L1`, followed by `CAP-SINGLE-POD-L2`; only then may `CAP-MIXED-B0` begin the `baseline_eligible=true` train.
