# Friend envelope closure — integration binding

## Exact heads

- Integration base: `54c6d0a15cd2e0555d309b59319f1ce30fab268c`
- Premerge evidence head: `7b629b9c8839d4c8085487bf9924eae4729c1d75`
- Integration Business Merge: `64b37d181828cf5b87dc65873d4370bc69f8a416`
- Manifest/evidence anchor: `9c2c924eedb0d58d373e7241aacdd18b46ffeb52`
- H5 integration: `525c4d324100b17a891f02797c4880fa9a02f70a`

The Manifest binds Business Merge and the immediately preceding evidence anchor. This document is a descendant evidence record and does not move either identity.

## Images

```text
Backend tag (local locator only): q1-friend-envelope-backend:64b37d1
Backend image digest: sha256:a273cd38f4cd06c3ab872094fd4a5e82a7d606d9c4d3b69b2f6c91de393135a9
H5 image digest:      sha256:9bfdedaa70f7cbf8bac86bb25cc2ff62dba1cf7832fb5d8f66c72c1f113486fd
Redis image digest:   sha256:2a51817f79255c8b69f86a974459c2e0359aff81417d80158f2b9e541e6f4b33
```

Backend runtime contains exact locally cross-compiled `player-svr`, `farm-svr`, `farm-worker`, and `farm-gate` binaries, non-root user, and the frozen runtime entrypoint. The normal in-container dependency download was unavailable because container DNS could not resolve the private module host; no source/test failure occurred. The repository-supported prebuilt pattern was used, and no build credential entered the image or evidence.

## Integration Gate

At Business Merge:

```text
go test ./... -count=1                         PASS
go vet ./...                                   PASS
go build ./cmd/...                             PASS
focused Friend/CAP Race                        PASS
backend runtime image build                    PASS
```

The checked-in runner now hard-fails unless the exact Business Commit, Evidence Head, Redis image, Backend image and H5 image are locally present with the frozen digests.

## Authorization

This binding authorizes post-fix L1 Harness Smoke only after Manifest preflight. It does not authorize skipping Smoke, declaring a baseline, entering L2, or claiming DAU capacity.

```text
baseline_eligible = false
capacity_claim = NONE
supports_30m_dau = false
```
