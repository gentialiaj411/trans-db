# NEXT_TASK.md

## Current Priority
Unblock and execute Raft persistence validation (build + targeted Raft tests).

## Why
Raft log and term/vote persistence code has been added, but this environment cannot currently validate it because CMake configure fails to locate `RocksDB` after regenerating from the flattened repo root.

## Strict Scope
1. Fix dependency/toolchain configuration so `cmake -B build -S .` succeeds from `C:\Users\bhask\Documents\PROJECTS\trans-db`.
2. Build Release target.
3. Run narrow tests for Raft only (including new persistence tests in `test/test_raft.cpp`).
4. Record pass/fail evidence in `CLAIMS_MATRIX.md` and `AUDIT_LOG.md`.

## After This Task
- Upgrade Raft persistence from flush-only writes to explicit OS-level durability (`fsync` / `FlushFileBuffers`) for both `raft_log` and `raft_meta`.
- Add serializability anomaly tests (write-skew, phantom reads) to tighten isolation claims.
