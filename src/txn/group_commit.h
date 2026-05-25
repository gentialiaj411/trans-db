#pragma once

#include "storage/status.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace txndb {

struct GroupCommitConfig {
  bool enabled{true};
  uint32_t window_us{500};
  uint32_t batch_size{32};
};

GroupCommitConfig LoadGroupCommitConfigFromEnv();

// Batches durable sync callbacks (WAL, coordinator log, raft log) by sync_key.
class DurableSyncRegistry {
public:
  static DurableSyncRegistry& Instance();

  Status QueueSync(const void* sync_key, std::function<Status()> flush_sync);

private:
  struct PendingSync {
    const void* key{nullptr};
    std::function<Status()> flush;
  };

  DurableSyncRegistry();
  ~DurableSyncRegistry();
  DurableSyncRegistry(const DurableSyncRegistry&) = delete;
  DurableSyncRegistry& operator=(const DurableSyncRegistry&) = delete;

  void WorkerLoop();

  GroupCommitConfig cfg_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::condition_variable worker_cv_;
  uint64_t synced_generation_{0};
  Status last_status_{Status::OK()};
  std::vector<PendingSync> pending_syncs_;
  std::thread worker_;
  std::atomic<bool> stop_{false};
};

}  // namespace txndb
