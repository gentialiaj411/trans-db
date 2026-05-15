#include "shard/shard_server.h"

#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"
#include "pgwire/pgwire_server.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif

using namespace txndb;

namespace {

std::atomic<bool> g_stop{false};

void OnSignal(int) { g_stop.store(true); }

uint32_t ParseUInt32(const char* s, uint32_t defv) {
  if (!s || !*s) {
    return defv;
  }
  return static_cast<uint32_t>(std::strtoul(s, nullptr, 10));
}

uint16_t ParseUInt16(const char* s, uint16_t defv) {
  if (!s || !*s) {
    return defv;
  }
  return static_cast<uint16_t>(std::strtoul(s, nullptr, 10));
}

}  // namespace

int main(int argc, char** argv) {
  uint32_t num_shards = 3;
  uint16_t pg_port = 5432;
  std::string data_dir = "./data";

  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "--shards" && i + 1 < argc) {
      num_shards = ParseUInt32(argv[++i], num_shards);
    } else if (a == "--port" && i + 1 < argc) {
      pg_port = ParseUInt16(argv[++i], pg_port);
    } else if (a == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    } else if (a == "--help" || a == "-h") {
      std::cout << "trans_db_server [--shards N] [--port P] [--data-dir D]\n";
      return 0;
    }
  }

  if (num_shards == 0) {
    std::cerr << "num_shards must be > 0\n";
    return 1;
  }

#ifdef _WIN32
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
#else
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
#endif

  std::vector<std::unique_ptr<ShardServer>> shard_servers;
  std::unordered_map<uint32_t, std::string> shard_addrs;

  for (uint32_t i = 0; i < num_shards; ++i) {
    const uint16_t port = static_cast<uint16_t>(50051 + i);
    const std::string addr = "0.0.0.0:" + std::to_string(port);
    const std::string sdir = data_dir + "/shard" + std::to_string(i);
    auto server = std::make_unique<ShardServer>(i, sdir, addr);
    server->Start();
    shard_addrs[i] = "localhost:" + std::to_string(port);
    shard_servers.push_back(std::move(server));
  }

  std::this_thread::sleep_for(std::chrono::seconds(2));

  Coordinator coordinator(shard_addrs, num_shards);
  Catalog catalog;

  PgWireServer pgwire(&coordinator, &catalog, pg_port);
  const Status st = pgwire.Start();
  if (!st.ok()) {
    std::cerr << "pgwire start failed: " << st.message() << "\n";
    return 1;
  }

  std::cout << "trans-db listening on port " << pgwire.ListenPort() << std::endl;
  std::cout << "Connect with: psql -h localhost -p " << pgwire.ListenPort() << std::endl;

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  pgwire.Stop();
  for (auto& s : shard_servers) {
    s->Stop();
  }
  return 0;
}
