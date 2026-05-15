#include "raft/raft_transport.h"

namespace txndb {

void InProcessTransport::RegisterNode(uint32_t node_id, RaftNode* node) {
  std::scoped_lock lk(mu_);
  nodes_[node_id] = node;
}

void InProcessTransport::DisconnectNode(uint32_t node_id) {
  std::scoped_lock lk(mu_);
  disconnected_.insert(node_id);
}

void InProcessTransport::ReconnectNode(uint32_t node_id, RaftNode* node) {
  std::scoped_lock lk(mu_);
  disconnected_.erase(node_id);
  nodes_[node_id] = node;
}

AppendEntriesResponse InProcessTransport::SendAppendEntries(uint32_t target_id,
                                                            const AppendEntriesRequest& req) {
  RaftNode* target = nullptr;
  {
    std::scoped_lock lk(mu_);
    if (disconnected_.count(req.leader_id) || disconnected_.count(target_id)) {
      AppendEntriesResponse resp{};
      resp.term = 0;
      resp.success = false;
      resp.match_index = 0;
      return resp;
    }
    auto it = nodes_.find(target_id);
    if (it == nodes_.end()) {
      AppendEntriesResponse resp{};
      resp.term = 0;
      resp.success = false;
      resp.match_index = 0;
      return resp;
    }
    target = it->second;
  }
  return target->HandleAppendEntries(req);
}

RequestVoteResponse InProcessTransport::SendRequestVote(uint32_t target_id,
                                                        const RequestVoteRequest& req) {
  RaftNode* target = nullptr;
  {
    std::scoped_lock lk(mu_);
    if (disconnected_.count(req.candidate_id) || disconnected_.count(target_id)) {
      RequestVoteResponse resp{};
      resp.term = 0;
      resp.vote_granted = false;
      return resp;
    }
    auto it = nodes_.find(target_id);
    if (it == nodes_.end()) {
      RequestVoteResponse resp{};
      resp.term = 0;
      resp.vote_granted = false;
      return resp;
    }
    target = it->second;
  }
  return target->HandleRequestVote(req);
}

}  // namespace txndb
