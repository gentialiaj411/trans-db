#include "cluster/cluster_launcher.h"
#include "bench/benchmark.h"
#include "bench/tpcc.h"
#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"

#include <chrono>
#include <cstdint>
#include <ctime>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
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
               "[--districts D] [--customers C] [--items I] [--data-dir PATH] "
               "[--transport grpc|inproc] "
               "[--mix standard|new-order-only|payment-only|order-status-only|delivery-only|stock-level-only]\n";
}

std::string EscapeJson(const std::string& s) {
  std::string o;
  o.reserve(s.size() + 8);
  for (char c : s) {
    if (c == '\\' || c == '"') {
      o.push_back('\\');
      o.push_back(c);
    } else if (c == '\n') {
      o.append("\\n");
    } else {
      o.push_back(c);
    }
  }
  return o;
}

std::string ReadGitCommit() {
  std::ifstream f(".git/HEAD");
  if (!f.is_open()) return "unknown";
  std::string head;
  std::getline(f, head);
  if (head.rfind("ref:", 0) == 0) {
    std::string ref = head.substr(5);
    std::ifstream rf(".git/" + ref);
    if (!rf.is_open()) return "unknown";
    std::string c;
    std::getline(rf, c);
    return c.empty() ? "unknown" : c;
  }
  return head.empty() ? "unknown" : head;
}

std::string NowIso8601() {
  using namespace std::chrono;
  const auto now = system_clock::now();
  const std::time_t tt = system_clock::to_time_t(now);
  std::tm tm{};
#ifdef _WIN32
  gmtime_s(&tm, &tt);
#else
  gmtime_r(&tt, &tm);
#endif
  char buf[64];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return std::string(buf);
}

tpcc::BenchConfig::MixMode ParseMixMode(const std::string& mix) {
  if (mix == "standard") return tpcc::BenchConfig::MixMode::Standard;
  if (mix == "new-order-only") return tpcc::BenchConfig::MixMode::NewOrderOnly;
  if (mix == "payment-only") return tpcc::BenchConfig::MixMode::PaymentOnly;
  if (mix == "order-status-only") return tpcc::BenchConfig::MixMode::OrderStatusOnly;
  if (mix == "delivery-only") return tpcc::BenchConfig::MixMode::DeliveryOnly;
  if (mix == "stock-level-only") return tpcc::BenchConfig::MixMode::StockLevelOnly;
  return tpcc::BenchConfig::MixMode::Standard;
}

}  // namespace

void EnsureBenchGroupCommitDefaults() {
  // Coordinator process: keep immediate durable sync (batched log records already coalesce writes).
#if defined(_WIN32)
  _putenv_s("TRANS_DB_GROUP_COMMIT_WINDOW_US", "0");
#else
  setenv("TRANS_DB_GROUP_COMMIT_WINDOW_US", "0", 1);
#endif
  if (std::getenv("TRANS_DB_GROUP_COMMIT_BATCH") == nullptr) {
#if defined(_WIN32)
    _putenv_s("TRANS_DB_GROUP_COMMIT_BATCH", "64");
#else
    setenv("TRANS_DB_GROUP_COMMIT_BATCH", "64", 1);
#endif
  }
}

int main(int argc, char* argv[]) {
  uint32_t num_shards = 3;
  uint32_t num_threads = 8;
  uint32_t duration_s = 30;
  uint32_t warmup_s = 5;
  tpcc::TPCCConfig tpcc_cfg;
  std::string mix = "standard";
  std::string data_dir = "./bench_data";
  ClusterTransport transport = ClusterTransport::Grpc;

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
    } else if (arg == "--items" && i + 1 < argc) {
      tpcc_cfg.items_per_wh = ParseU32(argv[++i], tpcc_cfg.items_per_wh);
    } else if (arg == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    } else if (arg == "--mix" && i + 1 < argc) {
      mix = argv[++i];
    } else if (arg == "--transport" && i + 1 < argc) {
      const std::string t = argv[++i];
      if (t == "inproc" || t == "inprocess") {
        transport = ClusterTransport::InProcess;
      } else {
        transport = ClusterTransport::Grpc;
      }
    } else if (arg == "--help" || arg == "-h") {
      PrintUsage();
      return 0;
    }
  }

  if (num_shards == 0 || num_threads == 0 || duration_s == 0 || tpcc_cfg.num_warehouses == 0 ||
      tpcc_cfg.districts_per_wh == 0 || tpcc_cfg.customers_per_dist == 0 ||
      tpcc_cfg.items_per_wh == 0) {
    std::cerr << "invalid configuration\n";
    return 1;
  }

  std::error_code ec;
  fs::create_directories(data_dir, ec);

  EnsureBenchGroupCommitDefaults();

  ClusterTopology topo;
  topo.num_shards = num_shards;

  ClusterLauncher cluster;
  cluster.Start(transport, data_dir, topo);

  const std::string coordinator_log = data_dir + "/coordinator.log";
  fs::remove(coordinator_log, ec);
  Coordinator coordinator(cluster.shard_entry_addresses(), num_shards, coordinator_log,
                          cluster.shard_replica_addresses());
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
  bcfg.num_shards = num_shards;
  bcfg.tpcc = tpcc_cfg;
  bcfg.mix_mode = ParseMixMode(mix);

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
            << ", OrderStatus: " << res.order_status_count << ", Delivery: " << res.delivery_count
            << ", StockLevel: " << res.stock_level_count << "\n";

  fs::create_directories("bench/results", ec);
  const std::string full_commit = ReadGitCommit();
  const std::string commit = full_commit.substr(0, std::min<size_t>(12, full_commit.size()));
  const std::string out_path = "bench/results/tpcc_style_full_mix_" + commit + ".json";
  std::ofstream jf(out_path, std::ios::binary | std::ios::trunc);
  if (jf.is_open()) {
    auto emit_txn = [&](const char* name, const tpcc::TxnMetrics& m) {
      jf << "\"" << name << "\":{"
         << "\"count\":" << m.committed << ","
         << "\"p50_us\":" << m.p50_us << ","
         << "\"p95_us\":" << m.p95_us << ","
         << "\"p99_us\":" << m.p99_us << ","
         << "\"success_rate\":" << m.success_rate << "}";
    };
    jf << "{";
    jf << "\"run_metadata\":{"
       << "\"commit\":\"" << EscapeJson(commit) << "\","
       << "\"host\":\"local\","
       << "\"date\":\"" << EscapeJson(NowIso8601()) << "\","
       << "\"build_flags\":\"Release\","
       << "\"cluster_topology\":\"" << num_shards << "_shards_"
       << (transport == ClusterTransport::Grpc ? "grpc_9proc" : "inproc") << "\""
       << "},";
    jf << "\"config\":{"
       << "\"warehouses\":" << tpcc_cfg.num_warehouses << ","
       << "\"threads\":" << num_threads << ","
       << "\"duration\":" << duration_s << ","
       << "\"warmup\":" << warmup_s << ","
       << "\"mix_ratios\":{\"new_order\":45,\"payment\":43,\"order_status\":4,\"delivery\":4,\"stock_level\":4}"
       << "},";
    jf << "\"per_txn_type\":{";
    emit_txn("new_order", res.new_order_metrics);
    jf << ",";
    emit_txn("payment", res.payment_metrics);
    jf << ",";
    emit_txn("order_status", res.order_status_metrics);
    jf << ",";
    emit_txn("delivery", res.delivery_metrics);
    jf << ",";
    emit_txn("stock_level", res.stock_level_metrics);
    jf << "},";
    jf << "\"overall_throughput_tps\":" << res.txns_per_second << ",";
    jf << "\"overall_p99_latency_us\":" << res.overall_p99_latency_us;
    jf << "}\n";
    std::cout << "Artifact: " << out_path << "\n";
  }

  if (transport == ClusterTransport::Grpc) {
    const char* gc_env = std::getenv("TRANS_DB_GROUP_COMMIT");
    const bool gc_on = !(gc_env && gc_env[0] == '0' && gc_env[1] == '\0');
    const std::string md_path = gc_on ? "bench/results/tpcc_group_commit.md" : "bench/results/tpcc_grpc.md";
    std::ofstream md(md_path, std::ios::trunc);
    if (md.is_open()) {
      md << "# TPC-C-style benchmark (gRPC Raft topology";
      if (gc_on) {
        md << ", group commit";
      }
      md << ")\n\n";
      md << "- Topology: " << num_shards << " shards x 3 Raft replicas (9 OS processes)\n";
      md << "- Transport: gRPC inter-replica Raft RPC\n";
      md << "- Group commit: " << (gc_on ? "enabled" : "disabled") << "\n";
      if (gc_on) {
        const char* win = std::getenv("TRANS_DB_GROUP_COMMIT_WINDOW_US");
        const char* bat = std::getenv("TRANS_DB_GROUP_COMMIT_BATCH");
        md << "- Group commit window_us: " << (win ? win : "0")
           << ", batch: " << (bat ? bat : "32") << "\n";
      }
      md << "- Throughput: " << res.txns_per_second << " txn/sec\n";
      md << "- p99 latency (overall): " << res.overall_p99_latency_us << " us\n";
      md << "- Threads: " << num_threads << ", duration: " << duration_s
         << "s, warmup: " << warmup_s << "s\n";
      md << "- Warehouses: " << tpcc_cfg.num_warehouses << "\n";
      md << "- Historical / superseded: see `bench/results/tpcc_grpc.md` (canonical P0 baseline section)\n";
      md << "- Date: " << NowIso8601() << "\n";
      std::cout << "Artifact: " << md_path << "\n";
    }
  }

  cluster.Stop();

  return 0;
}
