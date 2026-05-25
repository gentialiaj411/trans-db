# Group-commit parameter sweep (gRPC 9-process topology)

**Host:** genti_laptop, Windows 11 (build 10.0.26200)  
**Date:** 2026-05-24  
**Harness:** `build3/Release/bench_tpcc.exe --shards 3 --threads 8 --duration 30 --warmup 5 --transport grpc`  
**Isolation:** fresh `--data-dir` per rep; `Get-Process trans_db_replica` empty before each run.

## Sweep table (median of 2 reps)

| Config ID | TRANS_DB_GROUP_COMMIT | Coord window (µs) | Replica window (µs) | Batch | Rep1 tps | Rep2 tps | Median tps | Median p99 (µs) |
|---|---|---|---|---|---:|---:|---:|---:|
| gc_off_clean | 0 | — | — | — | 158.63 | 116.41 | **137.52** | 460149 |
| gc_w0_r0 | 1 | 0 | 0 | 64 | 63.94 | 97.03 | 80.49 | 631259 |
| gc_w0_r50 | 1 | 0 | 50 | 64 | 19.22 | 56.20 | 37.71 | 1255033 |
| gc_w0_r100 | 1 | 0 | 100 | 64 | 96.60 | 21.94 | 59.27 | 1015881 |
| gc_w0_r250 | 1 | 0 | 250 | 64 | 53.08 | 100.95 | 77.02 | 680653 |
| gc_w50_r100 | 1 | 50 | 100 | 128 | 45.93 | 95.10 | 70.52 | 875024 |
| gc_w100_r250 | 1 | 100 | 250 | 128 | 19.98 | 53.86 | 36.92 | 1534937 |

Raw JSON: `bench/results/group_commit_sweep_raw.json`

## Best config vs baseline

- **Baseline (gc_off_clean median):** 137.52 txn/sec  
- **Best group-commit config (highest median tps):** gc_w0_r250 at 77.02 txn/sec  
- **Speedup vs gc_off_clean:** 0.56× (slower; **does not meet ≥3× target**)

## Notes

- High rep-to-rep variance on several GC-on rows; kill stray `trans_db_replica` and use a clean data directory before trusting a single run.
- Canonical P0 headline for repo docs comes from a dedicated baseline run in `bench/results/tpcc_grpc.md` (115.95 txn/sec, 2026-05-24), not the sweep’s gc_off median.
