# QQFarm cold thick login: 1500/s priority split-6

Date: 2026-08-18

## Verdict

**1500/s launch rate was reached and all six load Pods completed, but the Friend Opportunity path hit its concurrency limit.** This is a valid diagnostic/capacity sample, not a clean pass for the full thick-login workflow.

The fixed 1024-shard backend remained healthy throughout the load. No load Pod or backend Pod was Pending or preempted during this stage.

## Deployment gate

- Runtime image: `farm-route-runtime@sha256:cf50fc92ac3dc39204859ec3a431a3ded79cc07b499f87500a1a076292c1b9f0`
- Load image: `farm-ws-stress@sha256:d3c5c8d0931953bd9137736f37f517c19f8c491ac2cb1e9c4e67284e1335c7e6`
- Route snapshot revision: `2049`
- Player: `512 + 512`
- Farm: `512 + 512`
- Every Holder: `installed_memberships=512`, `pending_actions=0`, `degraded_shards=0`, `serving_shard_count=512`
- Player/Farm/Worker/Gate/Route and all six load Pods: `high-cpu-priority`

## Load configuration

Six Pods ran at 250/s each for 300 seconds. The six ranges are disjoint and were not reused.

| Job | ID range | Run ID |
|---|---:|---|
| 1 | 977000000-977074999 | `dkrw8r5ln9u2` |
| 2 | 977075000-977149999 | `dkrw8qex40j7` |
| 3 | 977150000-977224999 | `dkrw8r4kddbk` |
| 4 | 977225000-977299999 | `dkrw8qbvae47` |
| 5 | 977300000-977374999 | `dkrw8r3z0rhz` |
| 6 | 977375000-977449999 | `dkrw8r2879sp` |

## Results

| Job | Attempts | Ready | Ready rate | Actual QPS | Ready P95 | Ready P99 |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 74,916 | 74,891 | 99.9666% | 249.590/s | 1201.43ms | 2023.66ms |
| 2 | 74,926 | 74,771 | 99.7931% | 249.104/s | 1214.85ms | 2017.85ms |
| 3 | 74,893 | 74,875 | 99.9759% | 249.523/s | 1197.85ms | 2007.31ms |
| 4 | 74,941 | 74,795 | 99.8052% | 249.055/s | 1194.40ms | 1971.75ms |
| 5 | 74,937 | 74,916 | 99.9720% | 249.675/s | 1200.64ms | 2024.78ms |
| 6 | 74,905 | 74,858 | 99.9373% | 249.476/s | 1335.91ms | 2799.28ms |
| **Total** | **449,518** | **449,106** | **99.9083%** | **1496.422/s** | — | — |

Top-level Ready failures: 412.

The thick-login sub-operation counters additionally reported:

- `ERR_OPPORTUNITY_BUSY` / opportunity HTTP 429: **8,547**
- friend opportunity context deadline: **813**
- login context deadline: **272**
- join rejected `ERR_SERVER_BUSY`: **228** combined across friend/thick-login join paths
- connect context deadline: **14**
- connect network timeout: **12**

The opportunity errors are asynchronous sub-operation errors and are not numerically identical to the 412 top-level Ready failures.

## Resource samples

Measured from `top.tsv` during the load.

| Pod | CPU avg | CPU max | Memory avg | Memory max |
|---|---:|---:|---:|---:|
| Gate | 1.494 cores | 1.910 cores | 97MiB | 130MiB |
| Player-0 | 0.890 cores | 1.159 cores | 2.01GiB | 3.66GiB |
| Player-1 | 1.304 cores | 1.730 cores | 2.06GiB | 3.93GiB |
| Farm-0 | 0.975 cores | 1.219 cores | 2.14GiB | 3.89GiB |
| Farm-1 | 0.907 cores | 1.156 cores | 2.12GiB | 3.95GiB |
| Worker-A | 0.055 cores | 0.056 cores | 22MiB | 24MiB |
| Worker-B | 0.064 cores | 0.066 cores | 21MiB | 23MiB |
| Route Controller | 0.293 cores | 0.302 cores | 37MiB | 38MiB |
| Load Pods | 0.321–0.508 cores avg | 0.432–0.656 cores max | 334–347MiB avg | 362–368MiB max |

Gate was close to its 2-core limit. Player-1 was the hottest Holder, while
Workers and Route Controller were not CPU bottlenecks.

## CPU pprof

CPU profiles were captured during the run for Gate, both Players, and both
Farms. SVG and PNG graphs are under `pprof/cpu/`.

### Gate

- `internal/runtime/syscall/linux.Syscall6`: 19.24% flat CPU
- `runtime.memmove`, protobuf unmarshal, allocation and GC were secondary
- CPU profile total samples: 55.57s over 30s, indicating roughly 1.85 cores

### Player

Both Player profiles showed the same shape:

- `internal/runtime/syscall/linux.Syscall6`: 12.05% / 12.93% flat
- `actor/activation.(*Controller).Stats`: 5.81% / 5.50% flat
- `runtime.memmove`: 5.12% / 4.75% flat
- allocation and GC scanning were significant

### Farm

Both Farm profiles showed:

- `internal/runtime/syscall/linux.Syscall6`: about 10.9% flat
- `runtime.memmove`: about 4.7% flat
- allocation, heap scanning and GC work
- no Worker-side CPU hotspot

## Heap pprof

Twenty-five non-empty heap profiles were captured during the run.

Latest samples showed:

- Player-0 in-use heap: about 2.33GiB; `Executor.mailboxFor` 81.01%
- Farm-0 in-use heap: about 2.73GiB; `Executor.mailboxFor` 65.60%
- Gate in-use heap: about 30MiB; buffered I/O dominated

The dominant memory-retention path remains Actor mailbox allocation/retention.

## Bottleneck assessment

The 1500/s bottleneck is no longer load-Pod scheduling or local ephemeral ports.
The main pressure is:

1. Gate CPU approaching its 2-core limit;
2. Friend Opportunity concurrency admission (`ERR_OPPORTUNITY_BUSY`);
3. Player/Farm allocation and GC activity;
4. Actor mailbox retention in heap.

Do not extrapolate this result to 2000/s. Before the next higher stage, either
raise or redesign the Opportunity admission path and keep the same priority
and multi-Pod preflight gate.

## Artifacts

- `job*.log`: six complete load results
- `top.tsv`: resource samples
- `pods.tsv`: Pod state/IP timeline
- `pprof/cpu/`: CPU profiles and SVG/PNG call graphs
- `pprof/heap/`: heap profiles
- `pprof-errors.log`: pprof collection errors
