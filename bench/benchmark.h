#pragma once

#include "bench/tpcc.h"
#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace txndb {
namespace tpcc {

struct BenchConfig {
  enum class MixMode { Standard, NewOrderOnly, PaymentOnly, OrderStatusOnly, DeliveryOnly, StockLevelOnly };
  uint32_t num_threads = 4;
  uint32_t duration_seconds = 30;
  uint32_t warmup_seconds = 5;
  uint32_t num_shards = 3;
  TPCCConfig tpcc;
  MixMode mix_mode = MixMode::Standard;
};

struct TxnMetrics {
  uint64_t committed = 0;
  uint64_t aborted = 0;
  double p50_us = 0;
  double p95_us = 0;
  double p99_us = 0;
  double success_rate = 0;
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
  uint64_t order_status_count = 0;
  uint64_t delivery_count = 0;
  uint64_t stock_level_count = 0;

  TxnMetrics new_order_metrics;
  TxnMetrics payment_metrics;
  TxnMetrics order_status_metrics;
  TxnMetrics delivery_metrics;
  TxnMetrics stock_level_metrics;
  double overall_p99_latency_us = 0;
};

class Benchmark {
public:
  Benchmark(Coordinator* coordinator, Catalog* catalog, const BenchConfig& config);
  BenchResult Run();

private:
  void WorkerThread(uint32_t thread_id);
  uint32_t ShardForKey(std::string_view key) const;
  bool CheckTxnTimeout(const char* txn_name, uint64_t txn_id, const char* step, uint32_t shard_id,
                       std::chrono::steady_clock::time_point start_tp) const;
  int PickTxnType(uint64_t r) const;
  static TxnMetrics BuildTxnMetrics(const std::vector<double>& samples, uint64_t committed,
                                    uint64_t aborted);
  bool RunNewOrder(uint32_t thread_id);
  bool RunPayment(uint32_t thread_id);
  bool RunOrderStatus(uint32_t thread_id);
  bool RunDelivery(uint32_t thread_id);
  bool RunStockLevel(uint32_t thread_id);

  Coordinator* coordinator_;
  Catalog* catalog_;
  BenchConfig config_;

  uint32_t warehouse_tid_{0};
  uint32_t district_tid_{0};
  uint32_t customer_tid_{0};
  uint32_t order_tid_{0};
  uint32_t order_line_tid_{0};
  uint32_t stock_tid_{0};

  std::atomic<bool> running_{false};
  std::atomic<bool> measuring_{false};
  std::atomic<uint64_t> committed_{0};
  std::atomic<uint64_t> aborted_{0};
  std::atomic<uint64_t> new_order_count_{0};
  std::atomic<uint64_t> payment_count_{0};
  std::atomic<uint64_t> order_status_count_{0};
  std::atomic<uint64_t> delivery_count_{0};
  std::atomic<uint64_t> stock_level_count_{0};
  std::atomic<uint64_t> new_order_abort_count_{0};
  std::atomic<uint64_t> payment_abort_count_{0};
  std::atomic<uint64_t> order_status_abort_count_{0};
  std::atomic<uint64_t> delivery_abort_count_{0};
  std::atomic<uint64_t> stock_level_abort_count_{0};

  std::mutex latency_mu_;
  std::vector<double> latencies_us_;
  std::vector<double> new_order_lat_us_;
  std::vector<double> payment_lat_us_;
  std::vector<double> order_status_lat_us_;
  std::vector<double> delivery_lat_us_;
  std::vector<double> stock_level_lat_us_;
};

}  // namespace tpcc
}  // namespace txndb
