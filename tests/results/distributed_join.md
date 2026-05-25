# Distributed JOIN (P3) — test results

**Date:** 2026-05-24  
**Platform:** Windows, gRPC cluster (`ClusterLauncher`, 1 replica per shard for tests)

## Scope

Cross-shard inner equi-join via physical operators:

- **Broadcast:** build side replicated to every shard; each shard probes local rows.
- **Shuffle:** both sides partitioned by `HashJoinKey(join_key) % num_shards`; local hash join per shard.

Env override: `TRANS_DB_JOIN_PHYSICAL=broadcast|shuffle|auto` (default `auto` uses row-count heuristic).

## Parity tests

`test_distributed_join` (`ctest -R distributed_join`):

| Case | Topology | Operator | Baseline |
|------|----------|----------|----------|
| `BroadcastMatchesSingleShardBaseline` | 2 shards vs 1 shard | `TRANS_DB_JOIN_PHYSICAL=broadcast` | Row-for-row match on `SELECT a.id, b.w ... ORDER BY b.w` |
| `ShuffleMatchesSingleShardBaseline` | 2 shards vs 1 shard | `TRANS_DB_JOIN_PHYSICAL=shuffle` | Same |

Data matches `test_e2e` `SqlInnerJoinMultiRowMatches` (tables `a`/`b`, ids 1–2 and 10–12) so rows hash across shards when `num_shards=2`.

**Local run (2026-05-24):** `test_distributed_join` — both tests **PASSED** (Release, Windows, gRPC, 3 replicas/shard).

## Implementation pointers

- Planner: `src/coordinator/distributed_join.{h,cpp}`
- Shard local join: `src/shard/distributed_join_handler.cpp`
- RPC: `DistributedJoin` in `proto/shard.proto`
- Executor hook: `ExecSelect` uses planner when `join` and `NumShards() > 1`

## Limits (unchanged)

- Inner equi-join only; no outer join, sort-merge, or cost-based optimizer.
- `auto` picks broadcast when `min(left_rows, right_rows) <= 128`.
