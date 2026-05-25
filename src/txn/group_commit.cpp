#include "txn/group_commit.h"

#include <chrono>
#include <cstdlib>
#include <unordered_map>

namespace txndb {

namespace {

bool EnvTruthy(const char* v, bool default_on) {
  if (v == nullptr || *v == '\0') {
    return default_on;
  }
  if (v[0] == '0' && v[1] == '\0') {
    return false;
  }
  return true;
}

uint32_t EnvU32(const char* name, uint32_t defv) {
  const char* v = std::getenv(name);
  if (v == nullptr || *v == '\0') {
    return defv;
  }
  return static_cast<uint32_t>(std::strtoul(v, nullptr, 10));
}

}  // namespace

GroupCommitConfig LoadGroupCommitConfigFromEnv() {
  GroupCommitConfig cfg;
  cfg.enabled = EnvTruthy(std::getenv("TRANS_DB_GROUP_COMMIT"), true);
  cfg.window_us = EnvU32("TRANS_DB_GROUP_COMMIT_WINDOW_US", 0);
  cfg.batch_size = EnvU32("TRANS_DB_GROUP_COMMIT_BATCH", 32);
  if (cfg.batch_size == 0) {
    cfg.batch_size = 1;
  }
  return cfg;
}

DurableSyncRegistry& DurableSyncRegistry::Instance() {
  static DurableSyncRegistry reg;
  return reg;
}

DurableSyncRegistry::DurableSyncRegistry() : cfg_(LoadGroupCommitConfigFromEnv()) {
  if (cfg_.enabled && cfg_.window_us > 0) {
    worker_ = std::thread([this] { WorkerLoop(); });
  }
}

DurableSyncRegistry::~DurableSyncRegistry() {
  if (!cfg_.enabled || cfg_.window_us == 0) {
    return;
  }
  stop_.store(true);
  worker_cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
}

void DurableSyncRegistry::WorkerLoop() {
  using clock = std::chrono::steady_clock;
  const auto window = std::chrono::microseconds(cfg_.window_us);

  while (!stop_.load()) {
    std::vector<PendingSync> batch;
    {
      std::unique_lock lk(mu_);
      worker_cv_.wait(lk, [this] { return stop_.load() || !pending_syncs_.empty(); });
      if (stop_.load() && pending_syncs_.empty()) {
        return;
      }

      const clock::time_point deadline = clock::now() + window;
      while (!stop_.load() && pending_syncs_.size() < cfg_.batch_size &&
             clock::now() < deadline) {
        worker_cv_.wait_until(lk, deadline);
      }
      batch.swap(pending_syncs_);
    }

    if (batch.empty()) {
      continue;
    }

    std::unordered_map<const void*, std::function<Status()>> unique_syncs;
    unique_syncs.reserve(batch.size());
    for (auto& item : batch) {
      unique_syncs[item.key] = std::move(item.flush);
    }

    Status merged = Status::OK();
    for (auto& [_key, cb] : unique_syncs) {
      (void)_key;
      const Status st = cb();
      if (!st.ok()) {
        merged = st;
      }
    }

    {
      std::scoped_lock lk(mu_);
      last_status_ = merged;
      synced_generation_++;
    }
    cv_.notify_all();
  }
}

Status DurableSyncRegistry::QueueSync(const void* sync_key, std::function<Status()> flush_sync) {
  if (!cfg_.enabled) {
    return flush_sync();
  }
  if (cfg_.window_us == 0) {
    return flush_sync();
  }

  uint64_t wait_generation = 0;
  {
    std::scoped_lock lk(mu_);
    wait_generation = synced_generation_;
    pending_syncs_.push_back(PendingSync{sync_key, std::move(flush_sync)});
    worker_cv_.notify_one();
  }

  std::unique_lock lk(mu_);
  cv_.wait(lk, [&] { return synced_generation_ > wait_generation; });
  return last_status_;
}

}  // namespace txndb
