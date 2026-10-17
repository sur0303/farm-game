# CAP-REDIS namespace Hash-Tag cleanup integration binding

- Integration base: `4000eedd37c97d41d9c43751a6484537203fd18e`
- Premerge evidence: `fa3cfbc2640946742c2abcd0e825a3f36e503daa`
- Integration evidence merge: `34d94b58b4cc941f184923b05cae79086c5d9da0`
- Frozen Business Commit remains: `93a66ce76d509d86e8cac8e1d87563d084eba821`
- Frozen backend image remains: `sha256:9498736750d89ece6cc511acf3f7fa243218ac1a2ce64f2806580f898a053ab7`
- Expected CAP binary: `bfed024275b2d2ccaa034a3737e1fb8007bfc387302408ef4f6560a35d61b83c`

Only CAP Harness and evidence changed. The runner now binds the new Evidence Head while retaining the reviewed product/image identity.

Integration Gate:

```text
go test ./...              PASS
go vet ./...               PASS
go build ./cmd/...         PASS
CAP Race                   PASS
focused Shard matrix       4800/4800/0 PASS
namespace residual         0
```

This authorizes another restart from Smoke only. Previous failed Warmup runs remain retained and ineligible.
