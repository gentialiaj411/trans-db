#pragma once

#include "storage/status.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace txndb {

class LockManager {
public:
  explicit LockManager(std::chrono::milliseconds default_timeout = std::chrono::milliseconds(5000));

  Status Acquire(uint32_t table_id, std::string_view key, uint64_t txn_id,
                 std::chrono::milliseconds timeout);

  Status Acquire(uint32_t table_id, std::string_view key, uint64_t txn_id);

  void Release(uint32_t table_id, std::string_view key, uint64_t txn_id);

  void ReleaseAll(uint64_t txn_id);

private:
  struct LockEntry {
    uint64_t owner{0};
    uint32_t hold_count{0};
    std::condition_variable cv;
    std::deque<uint64_t> wait_queue;
  };

  std::string MakeLockKey(uint32_t table_id, std::string_view key) const;

  void ReleaseInternal(const std::string& lk);

  std::mutex mu_;
  std::unordered_map<std::string, LockEntry> locks_;
  std::unordered_map<uint64_t, std::vector<std::string>> txn_locks_;
  std::chrono::milliseconds default_timeout_;
};

}  // namespace txndb
