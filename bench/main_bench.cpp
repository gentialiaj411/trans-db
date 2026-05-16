#include "shard/shard_server.h"
#include "bench/benchmark.h"
#include "bench/tpcc.h"
#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using namespace txndb;

namespace {

uint32_t ParseU32(const char* s, uint32_t defv) {
  if (!s || !*s) {
    return defv;
  }
  return static_cast<uint32_t>(std::strtoul(s, nullptr, 10));
}

void PrintUsage() {
  std::cout << "bench_tpcc [--shards N] [--threads T] [--duration S] [--warmup S] [--warehouses W] "
               "[--districts D] [--customers C] [--data-dir PATH]\n";
}

}  // namespace

int main(int argc, char* argv[]) {
  uint32_t num_shards = 3;
  uint32_t num_threads = 4;
  uint32_t duration_s = 30;
  uint32_t warmup_s = 5;
  tpcc::TPCCConfig tpcc_cfg;
  std::string data_dir = "./bench_data";

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--shards" && i + 1 < argc) {
      num_shards = ParseU32(argv[++i], num_shards);
    } else if (arg == "--threads" && i + 1 < argc) {
      num_threads = ParseU32(argv[++i], num_threads);
    } else if (arg == "--duration" && i + 1 < argc) {
      duration_s = ParseU32(argv[++i], duration_s);
    } else if (arg == "--warmup" && i + 1 < argc) {
      warmup_s = ParseU32(argv[++i], warmup_s);
    } else if (arg == "--warehouses" && i + 1 < argc) {
      tpcc_cfg.num_warehouses = ParseU32(argv[++i], tpcc_cfg.num_warehouses);
    } else if (arg == "--districts" && i + 1 < argc) {
      tpcc_cfg.districts_per_wh = ParseU32(argv[++i], tpcc_cfg.districts_per_wh);
    } else if (arg == "--customers" && i + 1 < argc) {
      tpcc_cfg.customers_per_dist = ParseU32(argv[++i], tpcc_cfg.customers_per_dist);
    } else if (arg == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      PrintUsage();
      return 0;
    }
  }

  if (num_shards == 0 || num_threads == 0 || duration_s == 0 || tpcc_cfg.num_warehouses == 0 ||
      tpcc_cfg.districts_per_wh == 0 || tpcc_cfg.customers_per_dist == 0) {
    std::cerr << "invalid configuration\n";
    return 1;
  }

  std::error_code ec;
  fs::create_directories(data_dir, ec);

  std::vector<std::unique_ptr<ShardServer>> shards;
  std::unordered_map<uint32_t, std::string> shard_addrs;

  for (uint32_t i = 0; i < num_shards; ++i) {
    const uint16_t port = static_cast<uint16_t>(57051 + i);
    const std::string addr = "127.0.0.1:" + std::to_string(port);
    const std::string shard_dir = data_dir + "/shard" + std::to_string(i);
    auto server = std::make_unique<ShardServer>(i, shard_dir, addr);
    server->Start();
    shard_addrs[i] = addr;
    shards.push_back(std::move(server));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  Coordinator coordinator(shard_addrs, num_shards);
  Catalog catalog;

  const auto load_start = std::chrono::steady_clock::now();
  const uint64_t loaded = tpcc::LoadTPCCData(&coordinator, &catalog, tpcc_cfg);
  const auto load_end = std::chrono::steady_clock::now();
  const double load_s =
      std::chrono::duration_cast<std::chrono::duration<double>>(load_end - load_start).count();

  tpcc::BenchConfig bcfg;
  bcfg.num_threads = num_threads;
  bcfg.duration_seconds = duration_s;
  bcfg.warmup_seconds = warmup_s;
  bcfg.tpcc = tpcc_cfg;

  tpcc::Benchmark bench(&coordinator, &catalog, bcfg);
  const tpcc::BenchResult res = bench.Run();

  const double abort_rate =
      (res.total_txns + res.total_aborts) > 0
          ? 100.0 * static_cast<double>(res.total_aborts) /
                static_cast<double>(res.total_txns + res.total_aborts)
          : 0.0;

  std::cout << std::fixed << std::setprecision(2);
  std::cout << "\n=== TPCC Benchmark (Simplified) ===\n";
  std::cout << "Shards: " << num_shards << ", Threads: " << num_threads
            << ", Warehouses: " << tpcc_cfg.num_warehouses << "\n";
  std::cout << "Rows loaded: " << loaded << " in " << load_s << " s\n";
  std::cout << "Throughput: " << res.txns_per_second << " txn/sec\n";
  std::cout << "Latency p50: " << res.p50_us << " us, p99: " << res.p99_us
            << " us, avg: " << res.avg_us << " us\n";
  std::cout << "Committed: " << res.total_txns << ", Aborted: " << res.total_aborts << " ("
            << abort_rate << "% abort rate)\n";
  std::cout << "New-Order: " << res.new_order_count << ", Payment: " << res.payment_count
            << "\n";

  for (auto& s : shards) {
    s->Stop();
  }

  return 0;
}
