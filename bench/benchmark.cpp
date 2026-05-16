#include "bench/benchmark.h"

#include "storage/status.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

namespace txndb {
namespace tpcc {

namespace {

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
  if (!w || !d || !c) {
    throw std::runtime_error("TPC-C tables are not loaded");
  }
  warehouse_tid_ = w->table_id;
  district_tid_ = d->table_id;
  customer_tid_ = c->table_id;
}

BenchResult Benchmark::Run() {
  committed_.store(0);
  aborted_.store(0);
  new_order_count_.store(0);
  payment_count_.store(0);
  {
    std::scoped_lock lk(latency_mu_);
    latencies_us_.clear();
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
    r.avg_us = sum / static_cast<double>(n);
  }

  return r;
}

void Benchmark::WorkerThread(uint32_t thread_id) {
  std::mt19937_64 rng(static_cast<uint64_t>(std::random_device{}()) + thread_id * 1315423911ULL);
  std::uniform_int_distribution<int> mix_dist(1, 100);

  while (running_.load(std::memory_order_relaxed)) {
    const bool run_new_order = mix_dist(rng) <= 60;
    const auto start = std::chrono::high_resolution_clock::now();
    const bool ok = run_new_order ? RunNewOrder(thread_id) : RunPayment(thread_id);
    const auto end = std::chrono::high_resolution_clock::now();

    if (ok) {
      committed_.fetch_add(1, std::memory_order_relaxed);
      if (run_new_order) {
        new_order_count_.fetch_add(1, std::memory_order_relaxed);
      } else {
        payment_count_.fetch_add(1, std::memory_order_relaxed);
      }
      if (measuring_.load(std::memory_order_relaxed)) {
        const double us =
            std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(end - start).count();
        std::scoped_lock lk(latency_mu_);
        latencies_us_.push_back(us);
      }
    } else {
      aborted_.fetch_add(1, std::memory_order_relaxed);
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

  const uint64_t txn = coordinator_->Begin();
  std::string district_row;
  if (!coordinator_->Read(txn, district_tid_, district_pk, &district_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
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

  std::string customer_row;
  if (!coordinator_->Read(txn, customer_tid_, customer_pk, &customer_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }

  std::string warehouse_row;
  if (!coordinator_->Read(txn, warehouse_tid_, warehouse_pk, &warehouse_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
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

  const Status cst = coordinator_->Commit(txn);
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

  const uint64_t txn = coordinator_->Begin();

  std::string warehouse_row;
  if (!coordinator_->Read(txn, warehouse_tid_, warehouse_pk, &warehouse_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
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

  std::string district_row;
  if (!coordinator_->Read(txn, district_tid_, district_pk, &district_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
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

  std::string customer_row;
  if (!coordinator_->Read(txn, customer_tid_, customer_pk, &customer_row).ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
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

  const Status cst = coordinator_->Commit(txn);
  if (!cst.ok()) {
    (void)coordinator_->Abort(txn);
    return false;
  }
  return true;
}

}  // namespace tpcc
}  // namespace txndb
