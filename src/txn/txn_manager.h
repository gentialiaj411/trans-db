#pragma once

#include "storage/mvcc_store.h"
#include "txn/lock_manager.h"
#include "txn/transaction.h"
#include "txn/wal.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace txndb {

class TxnManager {
public:
  TxnManager(MVCCStore* store, WAL* wal, LockManager* lock_mgr);

  uint64_t Begin(uint64_t snapshot_ts);

  Status Read(uint64_t txn_id, uint32_t table_id, std::string_view key, std::string* value);

  Status Scan(uint64_t txn_id, uint32_t table_id, std::string_view range_start_pk,
              std::string_view range_end_exclusive, bool range_end_open,
              std::vector<std::pair<std::string, std::string>>* rows_out);

  Status Write(uint64_t txn_id, uint32_t table_id, std::string_view key, std::string_view value);

  Status Delete(uint64_t txn_id, uint32_t table_id, std::string_view key);

  Status Prepare(uint64_t txn_id, uint64_t commit_ts);

  Status Commit(uint64_t txn_id, uint64_t commit_ts);

  Status Abort(uint64_t txn_id);

  Status CommitSingleShard(uint64_t txn_id, uint64_t commit_ts);

  Status Recover();

  Transaction* GetTxn(uint64_t txn_id);

private:
  Status ValidateReadSet(const Transaction& txn, uint64_t commit_ts);
  void ApplyWrites(Transaction& txn, uint64_t commit_ts);

  MVCCStore* store_{nullptr};
  WAL* wal_{nullptr};
  LockManager* lock_mgr_{nullptr};

  std::atomic<uint64_t> next_txn_id_{1};
  std::mutex mu_;
  std::unordered_map<uint64_t, std::unique_ptr<Transaction>> txns_;
};

}  // namespace txndb
