#pragma once

#include "cluster/cluster_config.h"
#include "shard/shard_server.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace txndb {

enum class ClusterTransport { Grpc, InProcess };

class ClusterLauncher {
public:
  ClusterLauncher() = default;
  ~ClusterLauncher();

  ClusterLauncher(const ClusterLauncher&) = delete;
  ClusterLauncher& operator=(const ClusterLauncher&) = delete;

  void Start(ClusterTransport transport, const std::string& data_dir, const ClusterTopology& topo,
             const std::string& host = "127.0.0.1", const std::string& replica_binary = {});

  void Stop();

  bool IsRunning() const { return running_; }

  const ClusterTopology& topology() const { return topo_; }

  std::unordered_map<uint32_t, std::string> shard_entry_addresses() const;

  std::unordered_map<uint32_t, std::vector<std::string>> shard_replica_addresses() const;

  ShardServer* inprocess_shard(uint32_t shard_id) const;

  void PartitionPeer(uint32_t shard_id, uint32_t peer_replica_id, bool partitioned);

  void StopShard(uint32_t shard_id);
  void StartShard(uint32_t shard_id);

private:
  void StartGrpcReplicas();
  void StartInProcessShards();

  bool running_{false};
  ClusterTransport transport_{ClusterTransport::Grpc};
  ClusterTopology topo_{};
  std::string data_dir_;
  std::string host_;
  std::string replica_binary_;

  struct ReplicaProc {
    uint32_t shard_id{0};
    uint32_t replica_id{0};
#ifdef _WIN32
    void* process{nullptr};
#else
    int pid{-1};
#endif
  };
  std::vector<ReplicaProc> children_;

  std::vector<std::unique_ptr<ShardServer>> inprocess_shards_;
};

}  // namespace txndb
