# CPU pprof graphs

These CPU profiles were captured during the 1500/s priority split-6 cold-login run.
They are generated with the matching runtime binaries and shown as SVG/PNG pprof call graphs.

Top observations:

- Gate: `internal/runtime/syscall/linux.Syscall6` dominates flat CPU (~19%), followed by protobuf/JSON and allocation work.
- Player: syscall time plus `actor/activation.(*Controller).Stats`, `memmove`, allocation and GC scanning.
- Farm: syscall, `memmove`, allocation and GC scanning; no single Worker hotspot.
