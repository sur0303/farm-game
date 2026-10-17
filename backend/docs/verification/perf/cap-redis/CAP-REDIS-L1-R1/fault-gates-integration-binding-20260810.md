# CAP-REDIS-L1-R1 Fault Gates — Integration Binding — 2026-08-10

- Integration parent: `1890e70b973f193f04992642a036d41699c0e654`
- Premerge head: `a34f830c99216edfc5afdd4e18ae02555172938d`
- Integration merge: `953f9efde14e30164914d59b064a6865a7a10810`
- Fault test head: `5fbbda4ec521c1ea7590f867dc2f7186b4ca307e`
- Focused review result: `2da2d7f4cda225846e347c034b515bcb653c3e06`
- Business commit: `93a66ce76d509d86e8cac8e1d87563d084eba821`
- Backend image: `sha256:9498736750d89ece6cc511acf3f7fa243218ac1a2ce64f2806580f898a053ab7`

This is a CAP test/evidence-only merge. Production code, Business Commit, Backend image, Manifest frozen identity and the measured binary remain unchanged. The existing Warmup/Formal result is not rerun or rewritten.

The hash-bound fault verdict now closes NOSCRIPT, connection reconstruction and post-write response-loss qualification. Summary regeneration remains fail-closed and reports exactly one required gap: the real end-to-end seven-command business amplification trace.

```text
fault_qualification=PASS
formal=5/5 PASS
business_command_amplification=NOT_MEASURED_REQUIRED_GAP
baseline_eligible=false
capacity_claim=NONE
supports_30m_dau=false
L2=BLOCKED
Mixed=BLOCKED
```
