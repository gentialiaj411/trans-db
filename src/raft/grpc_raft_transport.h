#pragma once

#include "raft/raft_node.h"
#include "raft.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace txndb {

class GrpcRaftTransport : public RaftTransport {
public:
  void AddPeer(uint32_t peer_id, const std::string& raft_addr);
  void SetLocalNodeId(uint32_t node_id) { local_node_id_ = node_id; }

  void DisconnectPeer(uint32_t peer_id);
  void ReconnectPeer(uint32_t peer_id);

  AppendEntriesResponse SendAppendEntries(uint32_t target_id,
                                          const AppendEntriesRequest& req) override;
  RequestVoteResponse SendRequestVote(uint32_t target_id,
                                      const RequestVoteRequest& req) override;

private:
  raft::RaftService::Stub* StubFor(uint32_t target_id);

  std::mutex mu_;
  uint32_t local_node_id_{UINT32_MAX};
  std::unordered_map<uint32_t, std::shared_ptr<grpc::Channel>> channels_;
  std::unordered_map<uint32_t, std::unique_ptr<raft::RaftService::Stub>> stubs_;
  std::unordered_set<uint32_t> partitioned_peers_;
  std::unordered_set<uint32_t> disconnected_local_;
};

}  // namespace txndb
