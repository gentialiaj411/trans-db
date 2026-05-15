#include "txn/lock_manager.h"

#include <algorithm>

namespace txndb {

namespace {

void AppendBigEndianU32(std::string* out, uint32_t v) {
  char buf[4];
  buf[0] = static_cast<char>((v >> 24) & 0xff);
  buf[1] = static_cast<char>((v >> 16) & 0xff);
  buf[2] = static_cast<char>((v >> 8) & 0xff);
  buf[3] = static_cast<char>(v & 0xff);
  out->append(buf, sizeof(buf));
}

}  // namespace

LockManager::LockManager(std::chrono::milliseconds default_timeout)
    : default_timeout_(default_timeout) {}

std::string LockManager::MakeLockKey(uint32_t table_id, std::string_view key) const {
  std::string out;
  out.reserve(4 + key.size());
  AppendBigEndianU32(&out, table_id);
  out.append(key);
  return out;
}

void LockManager::ReleaseInternal(const std::string& lk) {
  auto it = locks_.find(lk);
  if (it == locks_.end()) {
    return;
  }
  LockEntry& e = it->second;
  if (e.hold_count == 0) {
    return;
  }
  --e.hold_count;
  if (e.hold_count > 0) {
    e.cv.notify_all();
    return;
  }
  e.owner = 0;

  while (!e.wait_queue.empty()) {
    uint64_t next = e.wait_queue.front();
    e.wait_queue.pop_front();
    e.owner = next;
    e.hold_count = 1;
    e.cv.notify_all();
    break;
  }

  e.cv.notify_all();
  if (e.hold_count == 0 && e.wait_queue.empty() && e.owner == 0) {
    locks_.erase(it);
  }
}

Status LockManager::Acquire(uint32_t table_id, std::string_view key, uint64_t txn_id) {
  return Acquire(table_id, key, txn_id, default_timeout_);
}

Status LockManager::Acquire(uint32_t table_id, std::string_view key, uint64_t txn_id,
                            std::chrono::milliseconds timeout) {
  const std::string lk = MakeLockKey(table_id, key);
  std::unique_lock<std::mutex> lock(mu_);

  LockEntry& e = locks_[lk];

  if (e.owner == txn_id) {
    ++e.hold_count;
    txn_locks_[txn_id].push_back(lk);
    return Status::OK();
  }

  if (e.owner == 0) {
    e.owner = txn_id;
    e.hold_count = 1;
    txn_locks_[txn_id].push_back(lk);
    return Status::OK();
  }

  e.wait_queue.push_back(txn_id);
  const auto deadline = std::chrono::steady_clock::now() + timeout;

  while (true) {
    if (e.owner == txn_id && e.hold_count > 0) {
      txn_locks_[txn_id].push_back(lk);
      return Status::OK();
    }
    if (e.cv.wait_until(lock, deadline) == std::cv_status::timeout) {
      if (e.owner == txn_id && e.hold_count > 0) {
        txn_locks_[txn_id].push_back(lk);
        return Status::OK();
      }
      auto it = std::find(e.wait_queue.begin(), e.wait_queue.end(), txn_id);
      if (it != e.wait_queue.end()) {
        e.wait_queue.erase(it);
      }
      e.cv.notify_all();
      return Status::Error(StatusCode::TimedOut, "lock acquire timeout");
    }
  }
}

void LockManager::Release(uint32_t table_id, std::string_view key, uint64_t txn_id) {
  const std::string lk = MakeLockKey(table_id, key);
  std::unique_lock<std::mutex> lock(mu_);
  auto it = locks_.find(lk);
  if (it == locks_.end()) {
    return;
  }
  LockEntry& e = it->second;
  if (e.owner != txn_id) {
    return;
  }
  auto& held = txn_locks_[txn_id];
  auto hit = std::find(held.begin(), held.end(), lk);
  if (hit == held.end()) {
    return;
  }
  held.erase(hit);
  if (held.empty()) {
    txn_locks_.erase(txn_id);
  }
  ReleaseInternal(lk);
}

void LockManager::ReleaseAll(uint64_t txn_id) {
  std::vector<std::string> keys;
  {
    std::unique_lock<std::mutex> lock(mu_);
    auto it = txn_locks_.find(txn_id);
    if (it == txn_locks_.end()) {
      return;
    }
    keys = std::move(it->second);
    txn_locks_.erase(it);
  }
  std::unique_lock<std::mutex> lock(mu_);
  for (const std::string& lk : keys) {
    auto lit = locks_.find(lk);
    if (lit == locks_.end()) {
      continue;
    }
    LockEntry& e = lit->second;
    if (e.owner != txn_id) {
      continue;
    }
    ReleaseInternal(lk);
  }
}

}  // namespace txndb
