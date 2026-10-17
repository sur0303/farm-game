# Friend hot fixture + Share Due filter integration binding

- Integration base/retained stop: `be41b4ea911ec8705e10c787f78f48829cbc49d7`
- Premerge evidence: `795ef07d9b8245729eb3f83002b0b430f3edd418`
- Business Merge: `93a66ce76d509d86e8cac8e1d87563d084eba821`
- Manifest/evidence anchor: `ae718ae18c92a98511fa3e944c2ae70cdc7285cf`
- H5 commit: `525c4d324100b17a891f02797c4880fa9a02f70a`

Images:

```text
Backend locator: q1-friend-hot-share-backend:93a66ce
Backend digest:  sha256:9498736750d89ece6cc511acf3f7fa243218ac1a2ce64f2806580f898a053ab7
H5 digest:       sha256:9bfdedaa70f7cbf8bac86bb25cc2ff62dba1cf7832fb5d8f66c72c1f113486fd
Redis digest:    sha256:2a51817f79255c8b69f86a974459c2e0359aff81417d80158f2b9e541e6f4b33
```

Integration Gate at Business Merge:

```text
go test ./... -count=1       PASS
go vet ./...                 PASS
go build ./cmd/...           PASS
focused Race                 PASS
runtime image build          PASS
```

The runner hard-binds the exact commits and image IDs. The earlier failed Warmup and setup stop remain retained. This binding authorizes another restart from Smoke only; it does not authorize resuming Warmup, baseline eligibility, L2 or Mixed.
