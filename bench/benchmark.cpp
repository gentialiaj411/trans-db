#include "bench/benchmark.h"

#include "storage/status.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

namespace txndb {
namespace tpcc {

namespace {
constexpr auto kTxnWallTimeout = std::chrono::seconds(5);

bool ReadU16LE(std::string_view* data, uint16_t* out) {
  if (data->size() < 2) {
    return false;
  }
  *out = static_cast<uint16_t>(static_cast<unsigned char>((*data)[0]) |
                               (static_cast<unsigned char>((*data)[1]) << 8));
  data->remove_prefix(2);
  return true;
}

bool ReadU32LE(std::string_view* data, uint32_t* out) {
  if (data->size() < 4) {
    return false;
  }
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v |= static_cast<uint32_t>(static_cast<unsigned char>((*data)[i])) << (8 * i);
  }
  *out = v;
  data->remove_prefix(4);
  return true;
}

void AppendU16LE(std::string* buf, uint16_t v) {
  buf->push_back(static_cast<char>(v & 0xFF));
  buf->push_back(static_cast<char>((v >> 8) & 0xFF));
}

void AppendU32LE(std::string* buf, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    buf->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
  }
}

std::vector<std::string> DecodeRow(std::string_view data) {
  uint16_t cols = 0;
  if (!ReadU16LE(&data, &cols)) {
    return {};
  }
  std::vector<std::string> out;
  out.reserve(cols);
  for (uint16_t i = 0; i < cols; ++i) {
    uint32_t n = 0;
    if (!ReadU32LE(&data, &n) || data.size() < n) {
      return {};
    }
    out.emplace_back(data.substr(0, n));
    data.remove_prefix(n);
  }
  return out;
}

std::string EncodeRow(const std::vector<std::string>& cells) {
  std::string out;
  AppendU16LE(&out, static_cast<uint16_t>(cells.size()));
  for (const auto& c : cells) {
    AppendU32LE(&out, static_cast<uint32_t>(c.size()));
    out.append(c);
  }
  return out;
}

double ParseDouble(const std::string& s) {
  return std::stod(s);
}

int64_t ParseInt(const std::string& s) {
  return std::stoll(s);
}

}  // namespace

Benchmark::Benchmark(Coordinator* coordinator, Catalog* catalog, const BenchConfig& config)
    : coordinator_(coordinator), catalog_(catalog), config_(config) {
  const TableDef* w = catalog_->GetTable("warehouse");
  const TableDef* d = catalog_->GetTable("district");
  const TableDef* c = catalog_->GetTable("customer");
  const TableDef* o = catalog_->GetTable("orders");
  const TableDef* ol = catalog_->GetTable("order_line");
  const TableDef* s = catalog_->GetTable("stock");
  if (!w || !d || !c || !o || !ol || !s) {
    throw std::runtime_error("TPC-C tables are not loaded");
  }
  warehouse_tid_ = w->table_id;
  district_tid_ = d->table_id;
  customer_tid_ = c->table_id;
  order_tid_ = o->table_id;
  order_line_tid_ = ol->table_id;
  stock_tid_ = s->table_id;
}

BenchResult Benchmark::Run() {
  committed_.store(0);
  aborted_.store(0);
  new_order_count_.store(0);
  payment_count_.store(0);
  order_status_count_.store(0);
  delivery_count_.store(0);
  stock_level_count_.store(0);
  new_order_abort_count_.store(0);
  payment_abort_count_.store(0);
  order_status_abort_count_.store(0);
  delivery_abort_count_.store(0);
  stock_level_abort_count_.store(0);
  {
    std::scoped_lock lk(latency_mu_);
    latencies_us_.clear();
    new_order_lat_us_.clear();
    payment_lat_us_.clear();
    order_status_lat_us_.clear();
    delivery_lat_us_.clear();
    stock_level_lat_us_.clear();
  }

  running_.store(true);
  measuring_.store(false);

  std::vector<std::thread> workers;
  workers.reserve(config_.num_threads);
  for (uint32_t i = 0; i < config_.num_threads; ++i) {
    workers.emplace_back([this, i]() { WorkerThread(i); });
  }

  if (config_.warmup_seconds > 0) {
    std::this_thread::sleep_for(std::chrono::seconds(config_.warmup_seconds));
  }

  const auto measure_start = std::chrono::high_resolution_clock::now();
  measuring_.store(true);
  std::this_thread::sleep_for(std::chrono::seconds(config_.duration_seconds));
  measuring_.store(false);
  running_.store(false);

  for (auto& t : workers) {
    t.join();
  }

  const auto measure_end = std::chrono::high_resolution_clock::now();
  const double elapsed =
      std::chrono::duration_cast<std::chrono::duration<double>>(measure_end - measure_start).count();

  BenchResult r;
  r.total_txns = committed_.load();
  r.total_aborts = aborted_.load();
  r.duration_seconds = elapsed;
  r.txns_per_second = elapsed > 0.0 ? static_cast<double>(r.total_txns) / elapsed : 0.0;
  r.new_order_count = new_order_count_.load();
  r.payment_count = payment_count_.load();
  r.order_status_count = order_status_count_.load();
  r.delivery_count = delivery_count_.load();
  r.stock_level_count = stock_level_count_.load();

  std::vector<double> samples;
  {
    std::scoped_lock lk(latency_mu_);
    samples = latencies_us_;
  }
  if (!samples.empty()) {
    std::sort(samples.begin(), samples.end());
    const size_t n = samples.size();
    const size_t p50_idx = std::min(n - 1, static_cast<size_t>(std::floor(0.50 * n)));
    const size_t p99_idx = std::min(n - 1, static_cast<size_t>(std::floor(0.99 * n)));
    double sum = 0.0;
    for (double v : samples) {
      sum += v;
    }
    r.p50_us = samples[p50_idx];
    r.p99_us = samples[p99_idx];
    r.overall_p99_latency_us = r.p99_us;
    r.avg_us = sum / static_cast<double>(n);
  }
  r.new_order_metrics = BuildTxnMetrics(new_order_lat_us_, r.new_order_count, new_order_abort_count_.load());
  r.payment_metrics = BuildTxnMetrics(payment_lat_us_, r.payment_count, payment_abort_count_.load());
  r.order_status_metrics =
      BuildTxnMetrics(order_status_lat_us_, r.order_status_count, order_status_abort_count_.load());
  r.delivery_metrics = BuildTxnMetrics(delivery_lat_us_, r.delivery_count, delivery_abort_count_.load());
  r.stock_level_metrics =
      BuildTxnMetrics(stock_level_lat_us_, r.stock_level_count, stock_level_abort_count_.load());

  return r;
}

int Benchmark::PickTxnType(uint64_t r) const {
  switch (config_.mix_mode) {
    case BenchConfig::MixMode::NewOrderOnly:
      return 0;
    case BenchConfig::MixMode::PaymentOnly:
      return 1;
    case BenchConfig::MixMode::OrderStatusOnly:
      return 2;
    case BenchConfig::MixMode::DeliveryOnly:
      return 3;
    case BenchConfig::MixMode::StockLevelOnly:
      return 4;
    case BenchConfig::MixMode::Standard:
    default:
      break;
  }
  if (r < 45) return 0;
  if (r < 88) return 1;
  if (r < 92) return 2;
  if (r < 96) return 3;
  return 4;
}

uint32_t Benchmark::ShardForKey(std::string_view key) const {
  constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  uint64_t hash = kFnvOffset;
  for (unsigned char c : key) {
    hash ^= static_cast<uint64_t>(c);
    hash *= kFnvPrime;
  }
  return static_cast<uint32_t>(hash % std::max<uint32_t>(1u, config_.num_shards));
}

bool Benchmark::CheckTxnTimeout(const char* txn_name, uint64_t txn_id, const char* step, uint32_t shard_id,
                                std::chrono::steady_clock::time_point start_tp) const {
  const auto elapsed = std::chrono::steady_clock::now() - start_tp;
  if (elapsed <= kTxnWallTimeout) {
    return false;
  }
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
  std::cerr << "[BENCH_TIMEOUT] txn_type=" << txn_name << " txn_id=" << txn_id
            << " step=" << step << " shard=" << shard_id << " elapsed_ms=" << ms << std::endl;
  std::abort();
}

TxnMetrics Benchmark::BuildTxnMetrics(const std::vector<double>& samples, uint64_t committed,
                                      uint64_t aborted) {
  TxnMetrics m;
  m.committed = committed;
  m.aborted = aborted;
  const uint64_t total = committed + aborted;
  m.success_rate = total == 0 ? 0.0 : (100.0 * static_cast<double>(committed) / static_cast<double>(total));
  if (samples.empty()) {
    return m;
  }
  std::vector<double> s = samples;
  std::sort(s.begin(), s.end());
  const size_t n = s.size();
  m.p50_us = s[std::min(n - 1, static_cast<size_t>(std::floor(0.50 * n)))];
  m.p95_us = s[std::min(n - 1, static_cast<size_t>(std::floor(0.95 * n)))];
  m.p99_us = s[std::min(n - 1, static_cast<size_t>(std::floor(0.99 * n)))];
  return m;
}

void Benchmark::WorkerThread(uint32_t thread_id) {
  std::mt19937_64 rng(static_cast<uint64_t>(std::random_device{}()) + thread_id * 1315423911ULL);
  std::uniform_int_distribution<int> mix_dist(0, 99);
  std::uniform_int_distribution<int> jitter_us(50, 300);
  uint32_t consecutive_aborts = 0;

  while (running_.load(std::memory_order_relaxed)) {
    const int txn_type = PickTxnType(static_cast<uint64_t>(mix_dist(rng)));
    const auto start = std::chrono::high_resolution_clock::now();
    bool ok = false;
    constexpr uint32_t kMaxAttempts = 8;
    for (uint32_t attempt = 0; attempt < kMaxAttempts && running_.load(std::memory_order_relaxed);
         ++attempt) {
      switch (txn_type) {
        case 0:
          ok = RunNewOrder(thread_id);
          break;
        case 1:
          ok = RunPayment(thread_id);
          break;
        case 2:
          ok = RunOrderStatus(thread_id);
          break;
        case 3:
          ok = RunDelivery(thread_id);
          break;
        default:
          ok = RunStockLevel(thread_id);
          break;
      }
      if (ok) {
        break;
      }
      const int sleep_us = (1 << std::min<uint32_t>(attempt + 1, 8)) * jitter_us(rng);
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
    }
    const auto end = std::chrono::high_resolution_clock::now();

    if (ok) {
      consecutive_aborts = 0;
      committed_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 0) new_order_count_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 1) payment_count_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 2) order_status_count_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 3) delivery_count_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 4) stock_level_count_.fetch_add(1, std::memory_order_relaxed);
      if (measuring_.load(std::memory_order_relaxed)) {
        const double us =
            std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(end - start).count();
        std::scoped_lock lk(latency_mu_);
        latencies_us_.push_back(us);
        if (txn_type == 0) new_order_lat_us_.push_back(us);
        if (txn_type == 1) payment_lat_us_.push_back(us);
        if (txn_type == 2) order_status_lat_us_.push_back(us);
        if (txn_type == 3) delivery_lat_us_.push_back(us);
        if (txn_type == 4) stock_level_lat_us_.push_back(us);
      }
    } else {
      aborted_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 0) new_order_abort_count_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 1) payment_abort_count_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 2) order_status_abort_count_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 3) delivery_abort_count_.fetch_add(1, std::memory_order_relaxed);
      if (txn_type == 4) stock_level_abort_count_.fetch_add(1, std::memory_order_relaxed);
      consecutive_aborts = std::min<uint32_t>(consecutive_aborts + 1, 8);
      const int base = 1 << consecutive_aborts;
      const int sleep_us = base * jitter_us(rng);
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
    }
  }
}

bool Benchmark::RunNewOrder(uint32_t thread_id) {
  thread_local std::mt19937_64 rng(static_cast<uint64_t>(std::random_device{}()) +
                                   thread_id * 2654435761ULL);
  std::uniform_int_distribution<uint32_t> wdist(1, config_.tpcc.num_warehouses);
  std::uniform_int_distribution<uint32_t> ddist(0, config_.tpcc.districts_per_wh - 1);
  std::uniform_int_distribution<uint32_t> cdist(1, config_.tpcc.customers_per_dist);
  std::uniform_real_distribution<double> adist(1.0, 5000.0);

  const uint32_t w = wdist(rng);
  const uint32_t d = ddist(rng);
  const uint32_t c = cdist(rng);
  const double amount = adist(rng);

  const std::string district_pk = FormatPK(static_cast<int64_t>(w) * 10 + d);
  const std::string customer_pk =
      FormatPK(static_cast<int64_t>(w) * 100000 + static_cast<int64_t>(d) * 10000 + c);
  const std::string warehouse_pk = FormatPK(w);
  const auto start_tp = std::chrono::steady_clock::now();

  const uint64_t txn = coordinator_->Begin();
  if (CheckTxnTimeout("new_order", txn, "begin", 0, start_tp)) return false;
  std::string district_row;
  if (!coordinator_->Read(txn, district_tid_, district_pk, &district_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("new_order", txn, "read_district", ShardForKey(district_pk), start_tp)) return false;
  auto dcols = DecodeRow(district_row);
  if (dcols.size() != 5) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  const int64_t next_o_id = ParseInt(dcols[3]);
  dcols[3] = std::to_string(next_o_id + 1);
  if (!coordinator_->Write(txn, district_tid_, district_pk, EncodeRow(dcols)).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("new_order", txn, "write_district", ShardForKey(district_pk), start_tp)) return false;

  std::string customer_row;
  if (!coordinator_->Read(txn, customer_tid_, customer_pk, &customer_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("new_order", txn, "read_customer", ShardForKey(customer_pk), start_tp)) return false;

  std::string warehouse_row;
  if (!coordinator_->Read(txn, warehouse_tid_, warehouse_pk, &warehouse_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("new_order", txn, "read_warehouse", ShardForKey(warehouse_pk), start_tp)) return false;
  auto wcols = DecodeRow(warehouse_row);
  if (wcols.size() != 3) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  const double ytd = ParseDouble(wcols[2]);
  wcols[2] = std::to_string(ytd + amount);
  if (!coordinator_->Write(txn, warehouse_tid_, warehouse_pk, EncodeRow(wcols)).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("new_order", txn, "write_warehouse", ShardForKey(warehouse_pk), start_tp)) return false;

  const Status cst = coordinator_->Commit(txn);
  if (CheckTxnTimeout("new_order", txn, "commit", 0, start_tp)) return false;
  if (!cst.ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  return true;
}

bool Benchmark::RunPayment(uint32_t thread_id) {
  thread_local std::mt19937_64 rng(static_cast<uint64_t>(std::random_device{}()) +
                                   thread_id * 11400714819323198485ULL);
  std::uniform_int_distribution<uint32_t> wdist(1, config_.tpcc.num_warehouses);
  std::uniform_int_distribution<uint32_t> ddist(0, config_.tpcc.districts_per_wh - 1);
  std::uniform_int_distribution<uint32_t> cdist(1, config_.tpcc.customers_per_dist);
  std::uniform_real_distribution<double> adist(1.0, 5000.0);

  const uint32_t w = wdist(rng);
  const uint32_t d = ddist(rng);
  const uint32_t c = cdist(rng);
  const double amount = adist(rng);

  const std::string district_pk = FormatPK(static_cast<int64_t>(w) * 10 + d);
  const std::string customer_pk =
      FormatPK(static_cast<int64_t>(w) * 100000 + static_cast<int64_t>(d) * 10000 + c);
  const std::string warehouse_pk = FormatPK(w);
  const auto start_tp = std::chrono::steady_clock::now();

  const uint64_t txn = coordinator_->Begin();
  if (CheckTxnTimeout("payment", txn, "begin", 0, start_tp)) return false;

  std::string district_row;
  if (!coordinator_->Read(txn, district_tid_, district_pk, &district_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("payment", txn, "read_district", ShardForKey(district_pk), start_tp)) return false;
  auto dcols = DecodeRow(district_row);
  if (dcols.size() != 5) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  dcols[4] = std::to_string(ParseDouble(dcols[4]) + amount);
  if (!coordinator_->Write(txn, district_tid_, district_pk, EncodeRow(dcols)).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("payment", txn, "write_district", ShardForKey(district_pk), start_tp)) return false;

  std::string warehouse_row;
  if (!coordinator_->Read(txn, warehouse_tid_, warehouse_pk, &warehouse_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("payment", txn, "read_warehouse", ShardForKey(warehouse_pk), start_tp)) return false;
  auto wcols = DecodeRow(warehouse_row);
  if (wcols.size() != 3) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  wcols[2] = std::to_string(ParseDouble(wcols[2]) + amount);
  if (!coordinator_->Write(txn, warehouse_tid_, warehouse_pk, EncodeRow(wcols)).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("payment", txn, "write_warehouse", ShardForKey(warehouse_pk), start_tp)) return false;

  std::string customer_row;
  if (!coordinator_->Read(txn, customer_tid_, customer_pk, &customer_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("payment", txn, "read_customer", ShardForKey(customer_pk), start_tp)) return false;
  auto ccols = DecodeRow(customer_row);
  if (ccols.size() != 5) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  ccols[4] = std::to_string(ParseDouble(ccols[4]) - amount);
  if (!coordinator_->Write(txn, customer_tid_, customer_pk, EncodeRow(ccols)).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("payment", txn, "write_customer", ShardForKey(customer_pk), start_tp)) return false;

  const Status cst = coordinator_->Commit(txn);
  if (CheckTxnTimeout("payment", txn, "commit", 0, start_tp)) return false;
  if (!cst.ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  return true;
}

bool Benchmark::RunOrderStatus(uint32_t thread_id) {
  thread_local std::mt19937_64 rng(static_cast<uint64_t>(std::random_device{}()) +
                                   thread_id * 1469598103934665603ULL);
  std::uniform_int_distribution<uint32_t> wdist(1, config_.tpcc.num_warehouses);
  std::uniform_int_distribution<uint32_t> ddist(0, config_.tpcc.districts_per_wh - 1);
  std::uniform_int_distribution<uint32_t> cdist(1, config_.tpcc.customers_per_dist);
  const uint32_t w = wdist(rng);
  const uint32_t d = ddist(rng);
  const uint32_t c = cdist(rng);
  const std::string customer_pk =
      FormatPK(static_cast<int64_t>(w) * 100000 + static_cast<int64_t>(d) * 10000 + c);
  const std::string order_pk =
      FormatPK(static_cast<int64_t>(w) * 100000000 + static_cast<int64_t>(d) * 1000000 + c);
  const auto start_tp = std::chrono::steady_clock::now();
  const uint64_t txn = coordinator_->Begin();
  if (CheckTxnTimeout("order_status", txn, "begin", 0, start_tp)) return false;
  std::string customer_row;
  if (!coordinator_->Read(txn, customer_tid_, customer_pk, &customer_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("order_status", txn, "read_customer", ShardForKey(customer_pk), start_tp))
    return false;
  std::string order_row;
  if (!coordinator_->Read(txn, order_tid_, order_pk, &order_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("order_status", txn, "read_order", ShardForKey(order_pk), start_tp))
    return false;
  const Status cst = coordinator_->Commit(txn);
  if (CheckTxnTimeout("order_status", txn, "commit", 0, start_tp)) return false;
  if (!cst.ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  return true;
}

bool Benchmark::RunDelivery(uint32_t thread_id) {
  thread_local std::mt19937_64 rng(static_cast<uint64_t>(std::random_device{}()) +
                                   thread_id * 1099511628211ULL);
  std::uniform_int_distribution<uint32_t> wdist(1, config_.tpcc.num_warehouses);
  const uint32_t w = wdist(rng);
  const auto start_tp = std::chrono::steady_clock::now();
  const uint64_t txn = coordinator_->Begin();
  if (CheckTxnTimeout("delivery", txn, "begin", 0, start_tp)) return false;
  const uint32_t district_count = std::min<uint32_t>(10, config_.tpcc.districts_per_wh);
  for (uint32_t d = 0; d < district_count; ++d) {
    std::uniform_int_distribution<uint32_t> cdist(1, config_.tpcc.customers_per_dist);
    const uint32_t c = cdist(rng);
    const std::string customer_pk =
        FormatPK(static_cast<int64_t>(w) * 100000 + static_cast<int64_t>(d) * 10000 + c);
    std::string row;
    if (!coordinator_->Read(txn, customer_tid_, customer_pk, &row).ok()) {
      (void)coordinator_->Abort(txn);
      return false;
    }
    if (CheckTxnTimeout("delivery", txn, "read_customer", ShardForKey(customer_pk), start_tp))
      return false;
    auto cols = DecodeRow(row);
    if (cols.size() != 5) {
      (void)coordinator_->Abort(txn);
      return false;
    }
    cols[4] = std::to_string(ParseDouble(cols[4]) + 1.0);
    if (!coordinator_->Write(txn, customer_tid_, customer_pk, EncodeRow(cols)).ok()) {
      (void)coordinator_->Abort(txn);
      return false;
    }
    if (CheckTxnTimeout("delivery", txn, "write_customer", ShardForKey(customer_pk), start_tp))
      return false;
  }
  const Status cst = coordinator_->Commit(txn);
  if (CheckTxnTimeout("delivery", txn, "commit", 0, start_tp)) return false;
  if (!cst.ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  return true;
}

bool Benchmark::RunStockLevel(uint32_t thread_id) {
  thread_local std::mt19937_64 rng(static_cast<uint64_t>(std::random_device{}()) +
                                   thread_id * 7809847782465536322ULL);
  std::uniform_int_distribution<uint32_t> wdist(1, config_.tpcc.num_warehouses);
  std::uniform_int_distribution<uint32_t> ddist(0, config_.tpcc.districts_per_wh - 1);
  std::uniform_int_distribution<uint32_t> idist(1, 1000);
  const uint32_t w = wdist(rng);
  const uint32_t item = idist(rng);
  const std::string stock_pk = FormatPK(static_cast<int64_t>(w) * 10000 + item);
  const auto start_tp = std::chrono::steady_clock::now();
  const uint64_t txn = coordinator_->Begin();
  if (CheckTxnTimeout("stock_level", txn, "begin", 0, start_tp)) return false;
  std::string stock_row;
  if (!coordinator_->Read(txn, stock_tid_, stock_pk, &stock_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("stock_level", txn, "read_stock", ShardForKey(stock_pk), start_tp))
    return false;
  const uint32_t d = ddist(rng);
  const uint32_t c = (item % std::max<uint32_t>(1, config_.tpcc.customers_per_dist)) + 1;
  const std::string order_line_pk =
      FormatPK(static_cast<int64_t>(w) * 100000000 + static_cast<int64_t>(d) * 1000000 + c);
  std::string ol_row;
  if (!coordinator_->Read(txn, order_line_tid_, order_line_pk, &ol_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  if (CheckTxnTimeout("stock_level", txn, "read_order_line", ShardForKey(order_line_pk), start_tp))
    return false;
  const Status cst = coordinator_->Commit(txn);
  if (CheckTxnTimeout("stock_level", txn, "commit", 0, start_tp)) return false;
  if (!cst.ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  return true;
}

}  // namespace tpcc
}  // namespace txndb
