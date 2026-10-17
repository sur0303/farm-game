# Heap pprof graphs

These are heap-retention call graphs generated from the latest non-empty heap pprof captured during the valid 1000/s priority split-4 run.

They are **not CPU flame graphs**. CPU pprof was not persisted during this run.

- `player-svr-heap-latest.svg/png`: Player heap retention; `Executor.mailboxFor` dominates.
- `farm-svr-heap-latest.svg/png`: Farm heap retention; `Executor.mailboxFor` dominates.
- `farm-gate-heap-latest.svg/png`: Gate heap retention; small compared with holders.
