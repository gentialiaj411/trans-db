#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace txndb {

struct ClusterTopology {
  uint32_t num_shards{3};
  uint32_t replicas_per_shard{3};
  uint32_t shard_base_port{57000};
  uint32_t raft_base_port{58000};
  uint32_t port_stride{10};
};

inline uint16_t ShardPort(const ClusterTopology& topo, uint32_t shard_id, uint32_t replica_id) {
  return static_cast<uint16_t>(topo.shard_base_port + shard_id * topo.port_stride + replica_id);
}

inline uint16_t RaftPort(const ClusterTopology& topo, uint32_t shard_id, uint32_t replica_id) {
  return static_cast<uint16_t>(topo.raft_base_port + shard_id * topo.port_stride + replica_id);
}

inline std::string HostPort(const std::string& host, uint16_t port) {
  return host + ":" + std::to_string(port);
}

std::unordered_map<uint32_t, std::vector<std::string>> BuildShardReplicaAddresses(
    const ClusterTopology& topo, const std::string& host = "127.0.0.1");

std::unordered_map<uint32_t, std::string> BuildShardEntryAddresses(
    const ClusterTopology& topo, const std::string& host = "127.0.0.1");

}  // namespace txndb
