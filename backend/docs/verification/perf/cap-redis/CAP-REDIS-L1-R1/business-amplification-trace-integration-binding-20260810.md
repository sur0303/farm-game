# CAP-REDIS-L1-R1 Business Trace Integration Binding

- Integration parent: `5b42a4f587263ab0ce8369330321345e240782c7`
- Premerge head: `25b5af98db18e974291de7f38dab02c485ab9f13`
- Integration merge: `2c7f0f98531ee2b68d33062b234c30d3ea8e758b`
- Selected trace: `capredis-business-e2e-20260810T072200Z`
- Trace aggregate: `3a7006f2122c2cd5d6ae8e2d9af6cbd97656459a339196f84b3a49bfcbe11f89`

`CAP-REDIS-L1-R1` now has `Warmup 1/1 PASS`, `Formal 5/5 PASS`, all three fault qualifications PASS, and the independently reviewed seven-command real H5 amplification trace PASS. The generated L1 summary is `PASS` with zero required gaps.

This is test/evidence-only. Business Commit, Backend image, H5 Commit, measured CAP binary and frozen Manifest remain unchanged.

```text
CAP-REDIS-L1-R1=PASS
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
CAP-SINGLE-POD-L2-R1=AUTHORIZED_NOT_RUN
CAP-MIXED-B0-R1=BLOCKED
```
