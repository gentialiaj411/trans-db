#include "cluster/cluster_launcher.h"
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

ClusterTransport ParseTransport(const char* s) {
  if (!s) {
    return ClusterTransport::Grpc;
  }
  const std::string v(s);
  if (v == "inproc" || v == "inprocess") {
    return ClusterTransport::InProcess;
  }
  return ClusterTransport::Grpc;
}

}  // namespace

int main(int argc, char** argv) {
  uint32_t num_shards = 3;
  uint16_t pg_port = 5432;
  std::string data_dir = "./data";
  ClusterTransport transport = ClusterTransport::Grpc;

  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "--shards" && i + 1 < argc) {
      num_shards = ParseUInt32(argv[++i], num_shards);
    } else if (a == "--port" && i + 1 < argc) {
      pg_port = ParseUInt16(argv[++i], pg_port);
    } else if (a == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    } else if (a == "--transport" && i + 1 < argc) {
      transport = ParseTransport(argv[++i]);
    } else if (a == "--help" || a == "-h") {
      std::cout << "trans_db_server [--shards N] [--port P] [--data-dir D] "
                   "[--transport grpc|inproc]\n";
      return 0;
    }
  }

  if (num_shards == 0) {
    std::cerr << "num_shards must be > 0\n";
    return 1;
  }

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  ClusterTopology topo;
  topo.num_shards = num_shards;

  ClusterLauncher cluster;
  cluster.Start(transport, data_dir, topo);

  Coordinator coordinator(cluster.shard_entry_addresses(), num_shards, "",
                          cluster.shard_replica_addresses());
  Catalog catalog;

  PgWireServer pgwire(&coordinator, &catalog, pg_port);
  const Status st = pgwire.Start();
  if (!st.ok()) {
    std::cerr << "pgwire start failed: " << st.message() << "\n";
    cluster.Stop();
    return 1;
  }

  std::cout << "trans-db listening on port " << pgwire.ListenPort() << std::endl;
  std::cout << "Connect with: psql -h localhost -p " << pgwire.ListenPort() << std::endl;
  if (transport == ClusterTransport::Grpc) {
    std::cout << "Cluster: " << num_shards << " shards x " << topo.replicas_per_shard
              << " replicas (gRPC Raft transport)\n";
  }

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  pgwire.Stop();
  cluster.Stop();
  return 0;
}
