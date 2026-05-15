#pragma once

#include <atomic>
#include <cstdint>

namespace txndb {

class TimestampOracle {
public:
  explicit TimestampOracle(uint64_t start = 1) : current_(start) {}

  uint64_t Now() { return current_.fetch_add(1, std::memory_order_relaxed); }

  uint64_t AllocateRange(uint64_t count) {
    return current_.fetch_add(count, std::memory_order_relaxed);
  }

  uint64_t Peek() const { return current_.load(std::memory_order_relaxed); }

private:
  std::atomic<uint64_t> current_;
};

}  // namespace txndb
