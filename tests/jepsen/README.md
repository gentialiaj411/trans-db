# Jepsen-style fault harness (P1)

Bank-transfer workload with Jepsen-like JSONL history (`invoke` / `ok` / `fail`), five fault scenarios, and an in-house serializability checker with a known-bad self-test.

## Components

| File | Role |
|------|------|
| `jepsen_worker.cpp` | Concurrent transfer workload + history log |
| `history_checker.py` | Bank invariant + register serializability checker |
| `faults.sh` | `iptables` / `tc netem` helpers inside compose |
| `run_scenarios.py` | Orchestrates scenarios + writes `results/*.md` |
| `run_jepsen.sh` | Docker build/up + full scenario run (Linux) |
| `run_local_smoke.ps1` | Windows-friendly smoke without iptables |

## Quick checks

```bash
python3 tests/jepsen/history_checker.py --self-test
cmake --build build --target jepsen_worker
ctest --test-dir build -C Release -R jepsen_history_checker_selftest
```

## Full suite (Linux + Docker)

```bash
bash tests/jepsen/run_jepsen.sh
```

Requires Docker, `NET_ADMIN` on `jepsen-runner`, and the P0 compose topology (`deploy/compose/docker-compose.yml`).

## Fault scenarios

1. `partition_2pc_prepare` — runner cannot reach `shard1-rep0`
2. `partition_raft_leader` — `shard0-rep0` raft port drop
3. `clock_skew_replica` — `date -s` on `shard1-rep1`
4. `duplicate_appendentries` — `TRANS_DB_RAFT_DUPLICATE_APPEND=1` on leaders
5. `slow_replica_network` — `tc netem` on `shard2-rep2`

Results land under `tests/jepsen/results/` (`SUMMARY.md`, per-scenario `*.md`, `*_history.jsonl`).
