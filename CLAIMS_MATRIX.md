# CLAIMS_MATRIX.md

| Claim | Current evidence | Risk level | Missing proof | Suggested verification |
|---|---|---|---|---|
| PostgreSQL wire protocol support | Stated in `README.md` and `CLAUDE.md`; `src/pgwire/` directory present | Medium | No direct protocol-level test evidence inspected in this pass | Inspect pgwire tests and run only pgwire suite (`ctest -R pgwire`) |
| SQL subset/parser/executor exists | Stated SQL subset in docs; `src/coordinator/` present | Medium | Parser grammar and execution behavior not inspected | Inspect parser/executor files and run parser/e2e suites |
| MVCC storage over RocksDB | Stated in docs; `src/storage/` present | Medium | No code-level confirmation of RocksDB integration in this pass | Verify storage layer includes RocksDB-backed MVCC implementation |
| Strict 2PL + serializable isolation | Stated in docs | High | No direct test-case mapping to serializability guarantees yet | Map txn/e2e/fault tests to serializable anomalies; run targeted cases |
| WAL / durability | Stated in docs (`txn` layer mentions WAL) | High | Crash/restart durability proof not validated | Identify WAL recovery tests and run fault/restart-focused cases |
| Raft-style replication | Stated in docs; `src/raft/` present | Medium | Leader election/log replication behavior not verified | Inspect raft tests and run only raft suite |
| Cross-shard 2PC | Stated in docs (coordinator-driven 2PC) | High | Atomicity under partial failure not verified | Inspect coordinator/shard prepare-commit flow; run focused multi-shard tests |
| TPC-C benchmark harness | Stated in docs; `bench/` present | Low | Benchmark implementation and result reproducibility not verified | Inspect bench entrypoint and run short benchmark only if requested |
| Fault/integration tests | Docs claim fault/e2e suites and counts | Medium | Test inventory/results not independently confirmed in this pass | List tests from build metadata and run narrow `ctest -R "fault|e2e"` |
