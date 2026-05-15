#include "coordinator/coordinator.h"

#include <algorithm>
#include <future>
#include <stdexcept>

namespace txndb {

namespace {

grpc::ChannelArguments MakeChannelArgs() {
  grpc::ChannelArguments opts;
  return opts;
}

Status FromExecuteResponse(bool grpc_ok, const ExecuteResponse* resp, std::string* value_out = nullptr) {
  if (!grpc_ok || resp == nullptr) {
    return Status::IOError("gRPC Execute failed");
  }
  if (!resp->ok()) {
    return Status::Error(static_cast<StatusCode>(resp->error_code()), resp->error_message());
  }
  if (value_out != nullptr && !resp->value().empty()) {
    *value_out = resp->value();
  }
  return Status::OK();
}

}  // namespace

void Coordinator::ApplyDeadline(grpc::ClientContext* ctx) {
  const auto deadline = std::chrono::system_clock::now() + kGrpcTimeout;
  ctx->set_deadline(deadline);
}

Coordinator::Coordinator(const std::unordered_map<uint32_t, std::string>& shard_addresses,
                         uint32_t num_shards)
    : num_shards_(num_shards) {
  for (const auto& [id, addr] : shard_addresses) {
    if (id >= num_shards) {
      throw std::invalid_argument("shard id out of range");
    }
    auto ch =
        grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), MakeChannelArgs());
    channels_[id] = std::move(ch);
    stubs_[id] = ShardService::NewStub(channels_[id]);
  }
}

uint32_t Coordinator::RouteShard(std::string_view key) const {
  constexpr uint64_t kFnvOffset = 14695981039346656037ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  uint64_t hash = kFnvOffset;
  for (unsigned char c : key) {
    hash ^= static_cast<uint64_t>(c);
    hash *= kFnvPrime;
  }
  return static_cast<uint32_t>(hash % std::max<uint32_t>(1u, num_shards_));
}

ShardService::Stub* Coordinator::GetStub(uint32_t shard_id) {
  auto it = stubs_.find(shard_id);
  return it != stubs_.end() ? it->second.get() : nullptr;
}

Status Coordinator::EnsureShardParticipant(CoordinatorTxn& txn, uint32_t shard_id) {
  if (txn.participant_shards.count(shard_id)) {
    return Status::OK();
  }
  ShardService::Stub* stub = GetStub(shard_id);
  if (!stub) {
    return Status::Error(StatusCode::InvalidArgument, "unknown shard id");
  }
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  ExecuteRequest req;
  req.set_raft_txn_id(txn.raft_txn_id);
  req.set_op(OP_BEGIN);
  req.set_snapshot_ts(txn.snapshot_ts);
  ExecuteResponse resp;
  const bool ok = stub->Execute(&ctx, req, &resp).ok();
  Status st = FromExecuteResponse(ok, &resp);
  if (!st.ok()) {
    return st;
  }
  txn.participant_shards.insert(shard_id);
  return Status::OK();
}

uint64_t Coordinator::Begin() {
  std::scoped_lock lk(mu_);
  const uint64_t raft_txn_id = next_txn_id_++;
  const uint64_t snap = ts_oracle_.Now();
  CoordinatorTxn txn;
  txn.raft_txn_id = raft_txn_id;
  txn.snapshot_ts = snap;
  txn.aborted = false;
  txns_[raft_txn_id] = std::move(txn);
  return raft_txn_id;
}

Status Coordinator::Read(uint64_t txn_id, uint32_t table_id, std::string_view key,
                         std::string* value) {
  std::scoped_lock lk(mu_);
  auto tit = txns_.find(txn_id);
  if (tit == txns_.end()) {
    return Status::Error(StatusCode::InvalidArgument, "unknown txn");
  }
  CoordinatorTxn& txn = tit->second;
  if (txn.aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  const uint32_t shard_id = RouteShard(key);
  Status es = EnsureShardParticipant(txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  ShardService::Stub* stub = GetStub(shard_id);
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  ExecuteRequest req;
  req.set_raft_txn_id(txn.raft_txn_id);
  req.set_op(OP_READ);
  req.set_table_id(table_id);
  req.set_key(key.data(), key.size());
  ExecuteResponse resp;
  const bool grpc_ok = stub->Execute(&ctx, req, &resp).ok();
  return FromExecuteResponse(grpc_ok, &resp, value);
}

Status Coordinator::Write(uint64_t txn_id, uint32_t table_id, std::string_view key,
                          std::string_view value) {
  std::scoped_lock lk(mu_);
  auto tit = txns_.find(txn_id);
  if (tit == txns_.end()) {
    return Status::Error(StatusCode::InvalidArgument, "unknown txn");
  }
  CoordinatorTxn& txn = tit->second;
  if (txn.aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  const uint32_t shard_id = RouteShard(key);
  Status es = EnsureShardParticipant(txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  ShardService::Stub* stub = GetStub(shard_id);
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  ExecuteRequest req;
  req.set_raft_txn_id(txn.raft_txn_id);
  req.set_op(OP_WRITE);
  req.set_table_id(table_id);
  req.set_key(key.data(), key.size());
  req.set_value(value.data(), value.size());
  ExecuteResponse resp;
  const bool grpc_ok = stub->Execute(&ctx, req, &resp).ok();
  return FromExecuteResponse(grpc_ok, &resp);
}

Status Coordinator::Delete(uint64_t txn_id, uint32_t table_id, std::string_view key) {
  std::scoped_lock lk(mu_);
  auto tit = txns_.find(txn_id);
  if (tit == txns_.end()) {
    return Status::Error(StatusCode::InvalidArgument, "unknown txn");
  }
  CoordinatorTxn& txn = tit->second;
  if (txn.aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  const uint32_t shard_id = RouteShard(key);
  Status es = EnsureShardParticipant(txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  ShardService::Stub* stub = GetStub(shard_id);
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  ExecuteRequest req;
  req.set_raft_txn_id(txn.raft_txn_id);
  req.set_op(OP_DELETE);
  req.set_table_id(table_id);
  req.set_key(key.data(), key.size());
  ExecuteResponse resp;
  const bool grpc_ok = stub->Execute(&ctx, req, &resp).ok();
  return FromExecuteResponse(grpc_ok, &resp);
}

Status Coordinator::Scan(uint64_t txn_id, uint32_t table_id, std::string_view range_start_pk,
                         std::string_view range_end_exclusive, bool range_end_open,
                         std::vector<std::pair<std::string, std::string>>* rows_out) {
  rows_out->clear();

  std::scoped_lock lk(mu_);
  auto tit = txns_.find(txn_id);
  if (tit == txns_.end()) {
    return Status::Error(StatusCode::InvalidArgument, "unknown txn");
  }
  CoordinatorTxn& txn = tit->second;
  if (txn.aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }

  for (uint32_t sid = 0; sid < num_shards_; ++sid) {
    Status es = EnsureShardParticipant(txn, sid);
    if (!es.ok()) {
      return es;
    }
  }


  for (uint32_t sid = 0; sid < num_shards_; ++sid) {
    ShardService::Stub* stub = GetStub(sid);
    if (!stub) {
      return Status::Error(StatusCode::InvalidArgument, "unknown shard id");
    }
    grpc::ClientContext ctx;
    ApplyDeadline(&ctx);
    ExecuteRequest req;
    req.set_raft_txn_id(txn.raft_txn_id);
    req.set_op(OP_SCAN);
    req.set_table_id(table_id);
    req.set_range_start_pk(range_start_pk.data(), range_start_pk.size());
    req.set_range_end_exclusive(range_end_exclusive.data(), range_end_exclusive.size());
    req.set_range_end_open(range_end_open);
    ExecuteResponse resp;
    const bool grpc_ok = stub->Execute(&ctx, req, &resp).ok();
    if (!grpc_ok) {
      return Status::IOError("gRPC Execute failed");
    }
    if (!resp.ok()) {
      return Status::Error(static_cast<StatusCode>(resp.error_code()), resp.error_message());
    }
    for (const ScanRow& row : resp.scan_rows()) {
      rows_out->emplace_back(row.pk(), row.row_value());
    }
  }

  std::sort(rows_out->begin(), rows_out->end(),
            [](const std::pair<std::string, std::string>& a,
               const std::pair<std::string, std::string>& b) { return a.first < b.first; });

  return Status::OK();
}

Status Coordinator::SingleShardCommit(uint64_t raft_txn_id, uint32_t shard_id,
                                     uint64_t commit_ts) {
  ShardService::Stub* stub = GetStub(shard_id);
  if (!stub) {
    return Status::IOError("missing stub");
  }
  grpc::ClientContext prep_ctx;
  ApplyDeadline(&prep_ctx);
  PrepareRequest pr;
  pr.set_raft_txn_id(raft_txn_id);
  pr.set_commit_ts(commit_ts);
  PrepareResponse presp;
  if (!stub->Prepare(&prep_ctx, pr, &presp).ok()) {
    return Status::IOError("Prepare RPC failed");
  }
  if (!presp.vote_commit()) {
    grpc::ClientContext abort_ctx;
    ApplyDeadline(&abort_ctx);
    AbortRequest ar;
    ar.set_raft_txn_id(raft_txn_id);
    AbortResponse aresp;
    (void)stub->Abort(&abort_ctx, ar, &aresp);
    return Status::Error(StatusCode::Conflict, presp.error_message());
  }

  grpc::ClientContext cctx;
  ApplyDeadline(&cctx);
  CommitRequest cr;
  cr.set_raft_txn_id(raft_txn_id);
  cr.set_commit_ts(commit_ts);
  CommitResponse cresp;
  if (!stub->Commit(&cctx, cr, &cresp).ok() || !cresp.ok()) {
    return Status::IOError("Commit RPC failed");
  }
  return Status::OK();
}

Status Coordinator::TwoPhaseCommit(uint64_t raft_txn_id, std::vector<uint32_t> parts,
                                   uint64_t commit_ts) {
  std::vector<std::future<std::pair<uint32_t, PrepareResponse>>> prep_futs;
  prep_futs.reserve(parts.size());
  for (uint32_t sid : parts) {
    prep_futs.push_back(std::async(std::launch::async, [this, raft_txn_id, sid, commit_ts]() {
      PrepareResponse presp;
      ShardService::Stub* stub = GetStub(sid);
      grpc::ClientContext ctx;
      this->ApplyDeadline(&ctx);
      PrepareRequest pr;
      pr.set_raft_txn_id(raft_txn_id);
      pr.set_commit_ts(commit_ts);
      if (stub) {
        (void)stub->Prepare(&ctx, pr, &presp).ok();
      }
      return std::pair<uint32_t, PrepareResponse>{sid, std::move(presp)};
    }));
  }

  bool all_vote_commit = true;
  for (auto& fu : prep_futs) {
    auto [_sid, presp] = fu.get();
    (void)_sid;
    if (!presp.vote_commit()) {
      all_vote_commit = false;
      break;
    }
  }

  if (!all_vote_commit) {
    std::vector<std::future<void>> abort_futs;
    for (uint32_t sid : parts) {
      abort_futs.push_back(std::async(std::launch::async, [this, raft_txn_id, sid]() {
        ShardService::Stub* stub = this->GetStub(sid);
        if (!stub) {
          return;
        }
        grpc::ClientContext ctx;
        this->ApplyDeadline(&ctx);
        AbortRequest ar;
        ar.set_raft_txn_id(raft_txn_id);
        AbortResponse aresp;
        (void)stub->Abort(&ctx, ar, &aresp).ok();
      }));
    }
    for (auto& f : abort_futs) {
      f.wait();
    }
    return Status::Error(StatusCode::Conflict, "2PC prepare failed");
  }

  std::vector<std::future<bool>> commit_futs;
  for (uint32_t sid : parts) {
    commit_futs.push_back(std::async(std::launch::async, [this, raft_txn_id, sid, commit_ts]() {
      ShardService::Stub* stub = this->GetStub(sid);
      if (!stub) {
        return false;
      }
      grpc::ClientContext ctx;
      this->ApplyDeadline(&ctx);
      CommitRequest cr;
      cr.set_raft_txn_id(raft_txn_id);
      cr.set_commit_ts(commit_ts);
      CommitResponse cresp;
      return stub->Commit(&ctx, cr, &cresp).ok() && cresp.ok();
    }));
  }
  for (auto& f : commit_futs) {
    if (!f.get()) {
      return Status::IOError("2PC commit failed");
    }
  }
  return Status::OK();
}

Status Coordinator::Commit(uint64_t txn_id) {
  std::scoped_lock lk(mu_);
  auto tit = txns_.find(txn_id);
  if (tit == txns_.end()) {
    return Status::Error(StatusCode::InvalidArgument, "unknown txn");
  }
  CoordinatorTxn& txn = tit->second;
  if (txn.aborted) {
    txns_.erase(tit);
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  if (txn.participant_shards.empty()) {
    txns_.erase(tit);
    return Status::OK();
  }
  std::vector<uint32_t> parts(txn.participant_shards.begin(), txn.participant_shards.end());
  const uint64_t raft_txn_id = txn.raft_txn_id;
  const uint64_t commit_ts = ts_oracle_.Now();

  Status out;
  if (parts.size() == 1) {
    out = SingleShardCommit(raft_txn_id, parts.front(), commit_ts);
  } else {
    out = TwoPhaseCommit(raft_txn_id, std::move(parts), commit_ts);
  }
  txns_.erase(txn_id);
  return out;
}

Status Coordinator::Abort(uint64_t txn_id) {
  std::vector<uint32_t> parts;
  uint64_t raft_tid = 0;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::OK();
    }
    raft_tid = tit->second.raft_txn_id;
    parts.assign(tit->second.participant_shards.begin(), tit->second.participant_shards.end());
    txns_.erase(tit);
  }

  std::vector<std::future<void>> futs;
  for (uint32_t sid : parts) {
    futs.push_back(std::async(std::launch::async, [this, sid, raft_tid]() {
      ShardService::Stub* stub = this->GetStub(sid);
      if (!stub) {
        return;
      }
      grpc::ClientContext ctx;
      this->ApplyDeadline(&ctx);
      AbortRequest ar;
      ar.set_raft_txn_id(raft_tid);
      AbortResponse aresp;
      (void)stub->Abort(&ctx, ar, &aresp).ok();
    }));
  }
  for (auto& f : futs) {
    f.wait();
  }
  return Status::OK();
}

}  // namespace txndb
