#pragma once

#include "raft/raft_node.h"
#include "raft.grpc.pb.h"
#include "raft.pb.h"

#include <grpcpp/grpcpp.h>

namespace txndb {

class RaftServiceImpl final : public raft::RaftService::Service {
public:
  explicit RaftServiceImpl(RaftNode* node) : node_(node) {}

  grpc::Status AppendEntries(grpc::ServerContext* context,
                             const raft::AppendEntriesRequest* request,
                             raft::AppendEntriesResponse* response) override;

  grpc::Status RequestVote(grpc::ServerContext* context,
                           const raft::RequestVoteRequest* request,
                           raft::RequestVoteResponse* response) override;

  grpc::Status InstallSnapshot(grpc::ServerContext* context,
                               const raft::InstallSnapshotRequest* request,
                               raft::InstallSnapshotResponse* response) override;

private:
  RaftNode* node_{nullptr};
};

}  // namespace txndb
