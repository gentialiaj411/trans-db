#include "raft/raft_grpc_service.h"

#include "raft/raft_proto_convert.h"

namespace txndb {

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext* /*context*/,
                                            const raft::AppendEntriesRequest* request,
                                            raft::AppendEntriesResponse* response) {
  if (!node_) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "raft node not ready");
  }
  AppendEntriesRequest req;
  FromProto(*request, &req);
  const AppendEntriesResponse resp = node_->HandleAppendEntries(req);
  ToProto(resp, response);
  return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext* /*context*/,
                                          const raft::RequestVoteRequest* request,
                                          raft::RequestVoteResponse* response) {
  if (!node_) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "raft node not ready");
  }
  RequestVoteRequest req;
  FromProto(*request, &req);
  const RequestVoteResponse resp = node_->HandleRequestVote(req);
  ToProto(resp, response);
  return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::InstallSnapshot(grpc::ServerContext* /*context*/,
                                              const raft::InstallSnapshotRequest* request,
                                              raft::InstallSnapshotResponse* response) {
  response->set_term(request->term());
  response->set_success(false);
  return grpc::Status::OK;
}

}  // namespace txndb
