#pragma once

#include "bench/tpcc.h"
#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

namespace txndb {
namespace tpcc {

struct BenchConfig {
  uint32_t num_threads = 4;
  uint32_t duration_seconds = 30;
  uint32_t warmup_seconds = 5;
  TPCCConfig tpcc;
};

struct BenchResult {
  uint64_t total_txns = 0;
  uint64_t total_aborts = 0;
  double duration_seconds = 0;
  double txns_per_second = 0;

  double p50_us = 0;
  double p99_us = 0;
  double avg_us = 0;

  uint64_t new_order_count = 0;
  uint64_t payment_count = 0;
};

class Benchmark {
public:
  Benchmark(Coordinator* coordinator, Catalog* catalog, const BenchConfig& config);
  BenchResult Run();

private:
  void WorkerThread(uint32_t thread_id);
  bool RunNewOrder(uint32_t thread_id);
  bool RunPayment(uint32_t thread_id);

  Coordinator* coordinator_;
  Catalog* catalog_;
  BenchConfig config_;

  uint32_t warehouse_tid_{0};
  uint32_t district_tid_{0};
  uint32_t customer_tid_{0};

  std::atomic<bool> running_{false};
  std::atomic<bool> measuring_{false};
  std::atomic<uint64_t> committed_{0};
  std::atomic<uint64_t> aborted_{0};
  std::atomic<uint64_t> new_order_count_{0};
  std::atomic<uint64_t> payment_count_{0};

  std::mutex latency_mu_;
  std::vector<double> latencies_us_;
};

}  // namespace tpcc
}  // namespace txndb
