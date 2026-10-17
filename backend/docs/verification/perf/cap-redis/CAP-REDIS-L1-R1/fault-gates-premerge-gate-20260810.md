# CAP-REDIS-L1-R1 Fault Gates — Premerge Gate — 2026-08-10

## Binding

- Integration base: `1890e70b973f193f04992642a036d41699c0e654`
- Independently reviewed head: `9e5c88a480749affa3ab80eb8c329a19c55c516d`
- Review result head: `2da2d7f4cda225846e347c034b515bcb653c3e06`
- Premerge candidate tested: `fda59ccc66f90be8918c7f1686b4552b355abe90`
- Business commit and Backend image remain `93a66ce76d509d86e8cac8e1d87563d084eba821` / `sha256:9498736750d89ece6cc511acf3f7fa243218ac1a2ce64f2806580f898a053ab7`.

## Gates

```text
go test ./... -count=1                                  PASS
go vet ./...                                             PASS
go build ./cmd/...                                       PASS
go test -race ./cmd/cap-redis                            PASS
go test -race ./internal/storage/redis/authority         PASS
python3 scripts/perf/cap-redis/test_summarize.py         PASS (7/7)
manifest preflight                                       PASS
real Redis reconnect + response-loss                     PASS (2/2)
real Redis NOSCRIPT + replay + exact cleanup             PASS (3/3)
CAP fault namespace post-cleanup DBSIZE                   0
```

The real Redis instances were unique, loopback-only and destroyed after use. No credential was persisted. No global script flush or database flush was issued.

## Result

```text
PREMERGE_GATE=PASS
P0=0 / P1=0 / P2=0
FAULT_SIGNAL=CLOSED
```

This merge adds test-only fault qualification and evidence validation. It does not alter production code, the measured CAP binary, the frozen Formal rounds, or the Backend image. The train remains `INCONCLUSIVE_REQUIRED_GAPS` with only the real seven-command end-to-end amplification trace outstanding; L2 and Mixed remain blocked.
