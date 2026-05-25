#include "shard/shard_server.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

using namespace txndb;

namespace {

std::atomic<bool> g_stop{false};

void OnSignal(int) { g_stop.store(true); }

uint32_t ParseU32(const char* s, uint32_t defv) {
  if (!s || !*s) {
    return defv;
  }
  return static_cast<uint32_t>(std::strtoul(s, nullptr, 10));
}

std::unordered_map<uint32_t, std::string> ParseRaftPeers(const std::string& spec) {
  std::unordered_map<uint32_t, std::string> peers;
  std::stringstream ss(spec);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (item.empty()) {
      continue;
    }
    const auto at = item.find('@');
    if (at == std::string::npos) {
      continue;
    }
    const uint32_t id = static_cast<uint32_t>(std::strtoul(item.substr(0, at).c_str(), nullptr, 10));
    peers[id] = item.substr(at + 1);
  }
  return peers;
}

}  // namespace

int main(int argc, char** argv) {
  uint32_t shard_id = 0;
  uint32_t replica_id = 0;
  std::string data_dir = "./data";
  std::string shard_listen = "0.0.0.0:57000";
  std::string raft_listen = "0.0.0.0:58000";
  std::string raft_peers_spec;
  uint32_t gc_window_us = 0;
  uint32_t gc_batch = 0;

  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "--shard" && i + 1 < argc) {
      shard_id = ParseU32(argv[++i], shard_id);
    } else if (a == "--replica" && i + 1 < argc) {
      replica_id = ParseU32(argv[++i], replica_id);
    } else if (a == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    } else if (a == "--shard-listen" && i + 1 < argc) {
      shard_listen = argv[++i];
    } else if (a == "--raft-listen" && i + 1 < argc) {
      raft_listen = argv[++i];
    } else if (a == "--raft-peers" && i + 1 < argc) {
      raft_peers_spec = argv[++i];
    } else if (a == "--gc-window-us" && i + 1 < argc) {
      gc_window_us = ParseU32(argv[++i], gc_window_us);
    } else if (a == "--gc-batch" && i + 1 < argc) {
      gc_batch = ParseU32(argv[++i], gc_batch);
    } else if (a == "--help" || a == "-h") {
      std::cout << "trans_db_replica --shard N --replica R --data-dir D "
                   "--shard-listen HOST:PORT --raft-listen HOST:PORT "
                   "--raft-peers id@host:port,...\n";
      return 0;
    }
  }

  const auto raft_peers = ParseRaftPeers(raft_peers_spec);

  if (gc_window_us > 0) {
    const std::string gc_window_str = std::to_string(gc_window_us);
    const std::string gc_batch_str = std::to_string(gc_batch);
#if defined(_WIN32)
    _putenv_s("TRANS_DB_GROUP_COMMIT", "1");
    _putenv_s("TRANS_DB_GROUP_COMMIT_WINDOW_US", gc_window_str.c_str());
    if (gc_batch > 0) {
      _putenv_s("TRANS_DB_GROUP_COMMIT_BATCH", gc_batch_str.c_str());
    }
#else
    setenv("TRANS_DB_GROUP_COMMIT", "1", 1);
    setenv("TRANS_DB_GROUP_COMMIT_WINDOW_US", gc_window_str.c_str(), 1);
    if (gc_batch > 0) {
      setenv("TRANS_DB_GROUP_COMMIT_BATCH", gc_batch_str.c_str(), 1);
    }
#endif
  }

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  ReplicaServer server(shard_id, replica_id, data_dir, shard_listen, raft_listen, raft_peers);
  server.Start();

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  server.Stop();
  return 0;
}
