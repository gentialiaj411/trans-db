# TPC-C-style benchmark (gRPC Raft topology)

## Headline

- **Throughput:** 115.954 txn/sec
- **p99 latency (overall):** 831681 µs

## Run configuration (P0 gRPC baseline, group commit off)

- Topology: 3 shards × 3 Raft replicas (9 OS processes)
- Transport: gRPC inter-replica Raft RPC
- `TRANS_DB_GROUP_COMMIT=0`
- Threads: 8, duration: 30s, warmup: 5s, warehouses: 2
- Data dir: `bench_data_canonical` (clean)
- Date: 2026-05-24T21:36:22Z
- Artifact JSON: `bench/results/tpcc_style_full_mix_7ab86ffe4ca1.json`

Command:

```powershell
$env:TRANS_DB_GROUP_COMMIT = "0"
.\build3\Release\bench_tpcc.exe --shards 3 --threads 8 --duration 30 --warmup 5 --transport grpc --data-dir .\bench_data_canonical
```

## Historical / superseded

| Label | Throughput | Notes |
|---|---:|---|
| Pre-canonicalization headline (same file, 2026-05-24) | 89.4824 txn/sec | GC off, 8 threads; conflated with group-commit-on runs |
| Earlier P0 body reference | 120.769 txn/sec | 4-thread era; cited before sweep |
| In-process Raft per shard (dev only) | 178.13 txn/sec | `bench/results/tpcc_style_full_mix_7ab86ffe4ca1.json` @ commit `7ab86ffe4ca1`, topology `3_shards_local` — **not** the distributed claim |
| README-era snapshot | 178.126 txn/sec | Superseded by gRPC 9-process canonical above |
| Group-commit sweep gc_off median (2026-05-24) | 137.52 txn/sec | Two reps, isolated data dirs; see `bench/results/group_commit_sweep.md` |

Do not cite superseded numbers as the current distributed throughput.
