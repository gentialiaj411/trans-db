#pragma once

#include "raft/raft_node.h"

#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace txndb {

class InProcessTransport : public RaftTransport {
public:
  void RegisterNode(uint32_t node_id, RaftNode* node);

  void DisconnectNode(uint32_t node_id);

  void ReconnectNode(uint32_t node_id, RaftNode* node);

  AppendEntriesResponse SendAppendEntries(uint32_t target_id,
                                            const AppendEntriesRequest& req) override;
  RequestVoteResponse SendRequestVote(uint32_t target_id, const RequestVoteRequest& req) override;

private:
  std::mutex mu_;
  std::unordered_map<uint32_t, RaftNode*> nodes_;
  // Simulated partition: node cannot originate or deliver RPCs.
  std::unordered_set<uint32_t> disconnected_;
};

}  // namespace txndb
