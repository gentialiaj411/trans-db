#include "raft/grpc_raft_transport.h"

#include "raft/raft_proto_convert.h"

#include <chrono>
#include <cstdlib>

namespace txndb {

namespace {

bool DuplicateAppendEnabled() {
  const char* v = std::getenv("TRANS_DB_RAFT_DUPLICATE_APPEND");
  return v != nullptr && v[0] == '1';
}

}  // namespace

void GrpcRaftTransport::AddPeer(uint32_t peer_id, const std::string& raft_addr) {
  std::scoped_lock lk(mu_);
  auto ch = grpc::CreateChannel(raft_addr, grpc::InsecureChannelCredentials());
  channels_[peer_id] = std::move(ch);
  stubs_[peer_id] = raft::RaftService::NewStub(channels_[peer_id]);
}

void GrpcRaftTransport::DisconnectPeer(uint32_t peer_id) {
  std::scoped_lock lk(mu_);
  partitioned_peers_.insert(peer_id);
}

void GrpcRaftTransport::ReconnectPeer(uint32_t peer_id) {
  std::scoped_lock lk(mu_);
  partitioned_peers_.erase(peer_id);
}

raft::RaftService::Stub* GrpcRaftTransport::StubFor(uint32_t target_id) {
  std::scoped_lock lk(mu_);
  auto it = stubs_.find(target_id);
  return it != stubs_.end() ? it->second.get() : nullptr;
}

AppendEntriesResponse GrpcRaftTransport::SendAppendEntries(uint32_t target_id,
                                                           const AppendEntriesRequest& req) {
  {
    std::scoped_lock lk(mu_);
    if (partitioned_peers_.count(target_id) || partitioned_peers_.count(req.leader_id) ||
        disconnected_local_.count(local_node_id_)) {
      return AppendEntriesResponse{};
    }
  }

  raft::RaftService::Stub* stub = nullptr;
  {
    std::scoped_lock lk(mu_);
    auto it = stubs_.find(target_id);
    if (it == stubs_.end()) {
      return AppendEntriesResponse{};
    }
    stub = it->second.get();
  }

  raft::AppendEntriesRequest pb_req;
  ToProto(req, &pb_req);
  raft::AppendEntriesResponse pb_resp;
  grpc::ClientContext ctx;
  const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
  ctx.set_deadline(deadline);
  grpc::Status st = stub->AppendEntries(&ctx, pb_req, &pb_resp);
  if (!st.ok()) {
    return AppendEntriesResponse{};
  }
  AppendEntriesResponse resp;
  FromProto(pb_resp, &resp);
  if (DuplicateAppendEnabled()) {
    grpc::ClientContext ctx2;
    const auto deadline2 = std::chrono::system_clock::now() + std::chrono::seconds(5);
    ctx2.set_deadline(deadline2);
    raft::AppendEntriesResponse pb_resp2;
    (void)stub->AppendEntries(&ctx2, pb_req, &pb_resp2);
  }
  return resp;
}

RequestVoteResponse GrpcRaftTransport::SendRequestVote(uint32_t target_id,
                                                         const RequestVoteRequest& req) {
  {
    std::scoped_lock lk(mu_);
    if (partitioned_peers_.count(target_id) || partitioned_peers_.count(req.candidate_id) ||
        disconnected_local_.count(local_node_id_)) {
      return RequestVoteResponse{};
    }
  }

  raft::RaftService::Stub* stub = nullptr;
  {
    std::scoped_lock lk(mu_);
    auto it = stubs_.find(target_id);
    if (it == stubs_.end()) {
      return RequestVoteResponse{};
    }
    stub = it->second.get();
  }

  raft::RequestVoteRequest pb_req;
  ToProto(req, &pb_req);
  raft::RequestVoteResponse pb_resp;
  grpc::ClientContext ctx;
  const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
  ctx.set_deadline(deadline);
  grpc::Status st = stub->RequestVote(&ctx, pb_req, &pb_resp);
  if (!st.ok()) {
    return RequestVoteResponse{};
  }
  RequestVoteResponse resp;
  FromProto(pb_resp, &resp);
  return resp;
}

}  // namespace txndb
