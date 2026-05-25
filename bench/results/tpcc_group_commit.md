# TPC-C-style benchmark (gRPC, group commit)

Group commit reduces coordinator-log fsyncs from 3 to 2 per commit and deduplicates per-`sync_key` syncs; **no measured throughput uplift on this host** (see sweep).

## Mechanism (verified)

1. Group-commit worker no longer holds the registry mutex during `condition_variable::wait`.
2. `QueueSync(sync_key, …)` runs at most one `fsync` per WAL / coordinator log / batch window per key.
3. Coordinator 2PC log batching: `AppendWrite` for `COMMITTING`+`COMMITTED`, durable `Flush` after `PREPARING` and after the final record (2 coordinator fsyncs per commit, was 3).
4. Replica tuning: `TRANS_DB_REPLICA_GROUP_COMMIT_WINDOW_US` / `TRANS_DB_GROUP_COMMIT_BATCH` passed to `trans_db_replica` via `--gc-window-us` / `--gc-batch`.

## Parameter sweep (2026-05-24)

Full table: `bench/results/group_commit_sweep.md`

- P0 gRPC baseline (GC off, sweep median): 137.52 txn/sec
- Best GC-on median in sweep: gc_w0_r250 at 77.02 txn/sec (0.56× baseline; **< 3×**)

## Bench env (for reproduction only; not a throughput claim)

```powershell
$env:TRANS_DB_GROUP_COMMIT = "1"
$env:TRANS_DB_GROUP_COMMIT_WINDOW_US = "0"
$env:TRANS_DB_REPLICA_GROUP_COMMIT_WINDOW_US = "100"
$env:TRANS_DB_GROUP_COMMIT_BATCH = "64"
.\build3\Release\bench_tpcc.exe --shards 3 --threads 8 --duration 30 --warmup 5 --transport grpc --data-dir .\bench_data_gc_fresh
```

Disable group commit for canonical P0 baseline: `TRANS_DB_GROUP_COMMIT=0` → writes `bench/results/tpcc_grpc.md`.

## Historical / superseded

| Label | Throughput | Notes |
|---|---:|---|
| Session table (pre-sweep) | ~14–101 txn/sec | Stale replicas / shared `bench_data` |
| Prior headline in this file | 89.4824 txn/sec | Moved to `tpcc_grpc.md` historical when GC was off |
