#include "cluster/cluster_config.h"
#include "cluster/cluster_launcher.h"
#include "coordinator/coordinator.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace txndb;

namespace {

constexpr uint32_t kTable = 42;
constexpr uint64_t kInitialBalance = 1000;

uint64_t NowUs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

std::string AccountKey(uint32_t id) { return "jepsen_acc_" + std::to_string(id); }

std::string BalanceRow(uint64_t bal) { return std::to_string(bal); }

bool ParseBalance(const std::string& row, uint64_t* out) {
  if (row.empty()) {
    return false;
  }
  try {
    *out = static_cast<uint64_t>(std::stoull(row));
    return true;
  } catch (...) {
    return false;
  }
}

struct HistoryWriter {
  explicit HistoryWriter(std::string path) : path_(std::move(path)) {
    if (!path_.empty()) {
      out_.open(path_, std::ios::trunc);
    }
  }

  void Emit(int process, std::string_view type, std::string_view fn,
            const std::vector<std::string>& value) {
    std::ostringstream line;
    line << "{\"time_us\":" << NowUs() << ",\"process\":" << process << ",\"type\":\""
         << type << "\",\"f\":\"" << fn << "\",\"value\":[";
    for (size_t i = 0; i < value.size(); ++i) {
      if (i > 0) {
        line << ',';
      }
      line << '"' << value[i] << '"';
    }
    line << "]}\n";
    const std::string s = line.str();
    std::scoped_lock lk(mu_);
    if (out_.is_open()) {
      out_ << s;
      out_.flush();
    } else {
      std::cout << s;
    }
  }

  std::string path_;
  std::ofstream out_;
  std::mutex mu_;
};

bool LoadShardAddrsFromEnv(std::unordered_map<uint32_t, std::string>* addrs,
                           std::unordered_map<uint32_t, std::vector<std::string>>* replicas) {
  const char* ext = std::getenv("JEPSEN_EXTERNAL_CLUSTER");
  if (!ext || ext[0] != '1') {
    return false;
  }
  const char* s0 = std::getenv("JEPSEN_SHARD0");
  const char* s1 = std::getenv("JEPSEN_SHARD1");
  const char* s2 = std::getenv("JEPSEN_SHARD2");
  if (!s0 || !s1 || !s2) {
    return false;
  }
  (*addrs)[0] = s0;
  (*addrs)[1] = s1;
  (*addrs)[2] = s2;
  const char* rep_env[] = {std::getenv("JEPSEN_SHARD0_REPLICAS"), std::getenv("JEPSEN_SHARD1_REPLICAS"),
                           std::getenv("JEPSEN_SHARD2_REPLICAS")};
  for (uint32_t sid = 0; sid < 3; ++sid) {
    const char* rep = rep_env[sid];
    if (rep && *rep) {
      std::stringstream ss(rep);
      std::string item;
      while (std::getline(ss, item, ',')) {
        if (!item.empty()) {
          (*replicas)[sid].push_back(item);
        }
      }
    } else {
      (*replicas)[sid].push_back((*addrs)[sid]);
    }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  uint32_t num_accounts = 12;
  uint32_t num_threads = 4;
  uint32_t duration_s = 15;
  std::string history_path = "tests/jepsen/results/workload_history.jsonl";
  std::string data_dir = "./jepsen_data";
  std::string coordinator_log = "./jepsen_data/coordinator.log";

  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "--accounts" && i + 1 < argc) {
      num_accounts = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
    } else if (a == "--threads" && i + 1 < argc) {
      num_threads = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
    } else if (a == "--duration" && i + 1 < argc) {
      duration_s = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
    } else if (a == "--history" && i + 1 < argc) {
      history_path = argv[++i];
    } else if (a == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
      coordinator_log = data_dir + "/coordinator.log";
    }
  }

  HistoryWriter hist(history_path);

  std::unordered_map<uint32_t, std::string> shard_addrs;
  std::unordered_map<uint32_t, std::vector<std::string>> replica_addrs;
  std::unique_ptr<ClusterLauncher> cluster;
  const bool external = LoadShardAddrsFromEnv(&shard_addrs, &replica_addrs);

  if (!external) {
    cluster = std::make_unique<ClusterLauncher>();
    ClusterTopology topo;
    cluster->Start(ClusterTransport::Grpc, data_dir, topo);
    shard_addrs = cluster->shard_entry_addresses();
    replica_addrs = cluster->shard_replica_addresses();
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  }

  Coordinator coordinator(shard_addrs, 3, coordinator_log, replica_addrs);

  for (uint32_t a = 0; a < num_accounts; ++a) {
    const uint64_t t = coordinator.Begin();
    const Status ws = coordinator.Write(t, kTable, AccountKey(a), BalanceRow(kInitialBalance));
    if (!ws.ok()) {
      std::cerr << "init write failed: " << ws.message() << "\n";
      return 1;
    }
    const Status cs = coordinator.Commit(t);
    if (!cs.ok()) {
      std::cerr << "init commit failed: " << cs.message() << "\n";
      return 1;
    }
  }

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> ok_transfers{0};
  std::atomic<uint64_t> fail_transfers{0};

  const auto end_at = std::chrono::steady_clock::now() + std::chrono::seconds(duration_s);

  std::vector<std::thread> workers;
  workers.reserve(num_threads);
  for (uint32_t tid = 0; tid < num_threads; ++tid) {
    workers.emplace_back([&, tid]() {
      std::mt19937 rng(tid + 1);
      std::uniform_int_distribution<uint32_t> acct_dist(0, num_accounts - 1);
      std::uniform_int_distribution<uint64_t> amt_dist(1, 5);
      while (!stop.load() && std::chrono::steady_clock::now() < end_at) {
        const uint32_t from = acct_dist(rng);
        uint32_t to = acct_dist(rng);
        if (to == from) {
          to = (from + 1) % num_accounts;
        }
        const uint64_t amt = amt_dist(rng);
        const std::string f = "transfer";
        hist.Emit(static_cast<int>(tid), "invoke", f,
                  {std::to_string(from), std::to_string(to), std::to_string(amt)});

        const uint64_t txn = coordinator.Begin();
        std::string row_from;
        std::string row_to;
        const Status rf = coordinator.Read(txn, kTable, AccountKey(from), &row_from);
        const Status rt = coordinator.Read(txn, kTable, AccountKey(to), &row_to);
        if (!rf.ok() || !rt.ok()) {
          (void)coordinator.Abort(txn);
          fail_transfers.fetch_add(1);
          hist.Emit(static_cast<int>(tid), "fail", f,
                    {std::to_string(from), std::to_string(to), std::to_string(amt)});
          continue;
        }
        uint64_t bal_from = 0;
        uint64_t bal_to = 0;
        if (!ParseBalance(row_from, &bal_from) || !ParseBalance(row_to, &bal_to) ||
            bal_from < amt) {
          (void)coordinator.Abort(txn);
          fail_transfers.fetch_add(1);
          hist.Emit(static_cast<int>(tid), "fail", f,
                    {std::to_string(from), std::to_string(to), std::to_string(amt)});
          continue;
        }
        const Status wf =
            coordinator.Write(txn, kTable, AccountKey(from), BalanceRow(bal_from - amt));
        const Status wt = coordinator.Write(txn, kTable, AccountKey(to), BalanceRow(bal_to + amt));
        if (!wf.ok() || !wt.ok()) {
          (void)coordinator.Abort(txn);
          fail_transfers.fetch_add(1);
          hist.Emit(static_cast<int>(tid), "fail", f,
                    {std::to_string(from), std::to_string(to), std::to_string(amt)});
          continue;
        }
        const Status cs = coordinator.Commit(txn);
        if (!cs.ok()) {
          fail_transfers.fetch_add(1);
          hist.Emit(static_cast<int>(tid), "fail", f,
                    {std::to_string(from), std::to_string(to), std::to_string(amt)});
          continue;
        }
        ok_transfers.fetch_add(1);
        hist.Emit(static_cast<int>(tid), "ok", f,
                  {std::to_string(from), std::to_string(to), std::to_string(amt)});

        if (tid == 0) {
          const uint64_t rt_txn = coordinator.Begin();
          std::string snap;
          if (coordinator.Read(rt_txn, kTable, AccountKey(from), &snap).ok()) {
            hist.Emit(0, "invoke", "read", {std::to_string(from)});
            hist.Emit(0, "ok", "read", {std::to_string(from), snap});
          } else {
            hist.Emit(0, "invoke", "read", {std::to_string(from)});
            hist.Emit(0, "fail", "read", {std::to_string(from)});
          }
          (void)coordinator.Commit(rt_txn);
        }
      }
    });
  }

  for (auto& th : workers) {
    th.join();
  }
  stop.store(true);

  if (cluster) {
    cluster->Stop();
  }

  std::cerr << "jepsen_worker ok=" << ok_transfers.load() << " fail=" << fail_transfers.load()
            << " history=" << history_path << "\n";
  return fail_transfers.load() > ok_transfers.load() ? 1 : 0;
}
