#pragma once

#include "raft/raft_node.h"
#include "raft.grpc.pb.h"

namespace txndb {

void ToProto(const AppendEntriesRequest& in, raft::AppendEntriesRequest* out);
void FromProto(const raft::AppendEntriesRequest& in, AppendEntriesRequest* out);

void ToProto(const AppendEntriesResponse& in, raft::AppendEntriesResponse* out);
void FromProto(const raft::AppendEntriesResponse& in, AppendEntriesResponse* out);

void ToProto(const RequestVoteRequest& in, raft::RequestVoteRequest* out);
void FromProto(const raft::RequestVoteRequest& in, RequestVoteRequest* out);

void ToProto(const RequestVoteResponse& in, raft::RequestVoteResponse* out);
void FromProto(const raft::RequestVoteResponse& in, RequestVoteResponse* out);

}  // namespace txndb
