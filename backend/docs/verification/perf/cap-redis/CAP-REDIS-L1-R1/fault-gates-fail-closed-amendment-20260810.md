# CAP-REDIS-L1-R1 Fault Gate Fail-Closed Amendment — 2026-08-10

Initial focused review of handoff head `37c7d9879bf17ecd24c7e26e70b00afc5bc2b092` found one P1 evidence-validation gap:

- `summarize.py` checked hashes for the attachments named by the verdict, but did not require the exact frozen attachment set or derive each asserted gate from the corresponding attachment content. A self-consistent but incomplete verdict could therefore clear the three fault-gap strings.

Amendment code/test head: `a134077365cf3dcf77683cee21c237b4faf20854`.

The amended loader now fails closed unless all of the following hold:

1. the attachment names are exactly:
   - `connection-and-response-loss.txt`
   - `noscript-and-idempotent-replay.txt`
   - `fault-gate-cleanup.txt`
   - `fault-gate-environment.txt`;
2. every SHA-256 matches;
3. the environment attachment binds the exact test head and loopback-isolated profile;
4. output contains the required NOSCRIPT reload, reconnect/readback, `TIMEOUT_UNKNOWN`, durable `COMMITTED` resolution, `ALREADY_COMMITTED`, exact cleanup, isolated teardown, no-flush, and no-traversal witnesses;
5. neither Go test output contains a failed test marker;
6. the derived checks exactly match every check asserted by the verdict.

Negative controls now cover tampered bytes, hash-bound but incomplete output, a false/missing check, wrong Business Commit, path escape, and a missing required attachment: `7/7 PASS`.

The real frozen attachments continue to validate and the summary still has exactly one gap—the real end-to-end business-command amplification trace. This amendment does not alter the CAP binary, Formal evidence, production code, eligibility, or capacity claims.
