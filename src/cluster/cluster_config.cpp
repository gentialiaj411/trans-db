#include "cluster/cluster_config.h"

namespace txndb {

std::unordered_map<uint32_t, std::vector<std::string>> BuildShardReplicaAddresses(
    const ClusterTopology& topo, const std::string& host) {
  std::unordered_map<uint32_t, std::vector<std::string>> out;
  for (uint32_t s = 0; s < topo.num_shards; ++s) {
    std::vector<std::string> addrs;
    addrs.reserve(topo.replicas_per_shard);
    for (uint32_t r = 0; r < topo.replicas_per_shard; ++r) {
      addrs.push_back(HostPort(host, ShardPort(topo, s, r)));
    }
    out[s] = std::move(addrs);
  }
  return out;
}

std::unordered_map<uint32_t, std::string> BuildShardEntryAddresses(
    const ClusterTopology& topo, const std::string& host) {
  std::unordered_map<uint32_t, std::string> out;
  for (uint32_t s = 0; s < topo.num_shards; ++s) {
    out[s] = HostPort(host, ShardPort(topo, s, 0));
  }
  return out;
}

}  // namespace txndb
