#include "coordinator/coordinator.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
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
                         uint32_t num_shards,
                         std::string coordinator_log_path)
    : num_shards_(num_shards) {
  if (coordinator_log_path.empty()) {
    coordinator_log_path = (std::filesystem::temp_directory_path() / "trans_db_coordinator.log").string();
  }
  if (!CoordinatorLog::Open(coordinator_log_path, &coordinator_log_).ok()) {
    throw std::runtime_error("failed to open coordinator log");
  }

  for (const auto& [id, addr] : shard_addresses) {
    if (id >= num_shards) {
      throw std::invalid_argument("shard id out of range");
    }
    auto ch =
        grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), MakeChannelArgs());
    channels_[id] = std::move(ch);
    stubs_[id] = ShardService::NewStub(channels_[id]);
  }
  Status rs = Recover();
  if (!rs.ok()) {
    throw std::runtime_error("coordinator recover failed: " + rs.message());
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
  auto txn = std::make_shared<CoordinatorTxn>();
  txn->raft_txn_id = raft_txn_id;
  txn->snapshot_ts = snap;
  txn->aborted = false;
  txns_[raft_txn_id] = std::move(txn);
  return raft_txn_id;
}

Status Coordinator::AppendCoordinatorRecord(CoordinatorLogRecordType type, uint64_t txn_id,
                                            const std::vector<uint32_t>& shards) {
  if (!coordinator_log_) {
    return Status::IOError("coordinator log unavailable");
  }
  CoordinatorLogRecord rec;
  rec.type = type;
  rec.txn_id = txn_id;
  rec.timestamp_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  rec.shards = shards;
  return coordinator_log_->Append(rec);
}

TxnStateCode Coordinator::QueryShardTxnState(uint32_t shard_id, uint64_t txn_id) {
  ShardService::Stub* stub = GetStub(shard_id);
  if (!stub) {
    return TxnStateCode::TXN_UNKNOWN;
  }
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  TxnStateRequest req;
  req.set_raft_txn_id(txn_id);
  TxnStateResponse resp;
  if (!stub->QueryTxnState(&ctx, req, &resp).ok() || !resp.ok()) {
    return TxnStateCode::TXN_UNKNOWN;
  }
  return resp.state();
}

Status Coordinator::DriveCommit(uint64_t txn_id, const std::vector<uint32_t>& shards) {
  const uint64_t commit_ts = ts_oracle_.Now();
  std::vector<std::future<bool>> commit_futs;
  commit_futs.reserve(shards.size());
  for (uint32_t sid : shards) {
    commit_futs.push_back(std::async(std::launch::async, [this, txn_id, sid, commit_ts]() {
      ShardService::Stub* stub = GetStub(sid);
      if (!stub) {
        return false;
      }
      grpc::ClientContext ctx;
      ApplyDeadline(&ctx);
      CommitRequest cr;
      cr.set_raft_txn_id(txn_id);
      cr.set_commit_ts(commit_ts);
      CommitResponse cresp;
      return stub->Commit(&ctx, cr, &cresp).ok() && cresp.ok();
    }));
  }
  for (auto& f : commit_futs) {
    if (!f.get()) {
      return Status::IOError("recovery commit failed");
    }
  }
  return Status::OK();
}

Status Coordinator::DriveAbort(uint64_t txn_id, const std::vector<uint32_t>& shards) {
  std::vector<std::future<void>> abort_futs;
  abort_futs.reserve(shards.size());
  for (uint32_t sid : shards) {
    abort_futs.push_back(std::async(std::launch::async, [this, txn_id, sid]() {
      ShardService::Stub* stub = GetStub(sid);
      if (!stub) {
        return;
      }
      grpc::ClientContext ctx;
      ApplyDeadline(&ctx);
      AbortRequest ar;
      ar.set_raft_txn_id(txn_id);
      AbortResponse aresp;
      (void)stub->Abort(&ctx, ar, &aresp).ok();
    }));
  }
  for (auto& f : abort_futs) {
    f.wait();
  }
  return Status::OK();
}

Status Coordinator::Recover() {
  if (!coordinator_log_) {
    return Status::OK();
  }

  struct TxnRecoveryState {
    std::vector<uint32_t> shards;
    bool saw_preparing{false};
    bool saw_committing{false};
    bool saw_committed{false};
    bool saw_aborting{false};
    bool saw_aborted{false};
  };
  std::unordered_map<uint64_t, TxnRecoveryState> states;
  Status rs = coordinator_log_->Replay([&](const CoordinatorLogRecord& rec) {
    auto& st = states[rec.txn_id];
    if (rec.type == CoordinatorLogRecordType::PREPARING) {
      st.saw_preparing = true;
      st.shards = rec.shards;
    } else if (rec.type == CoordinatorLogRecordType::COMMITTING) {
      st.saw_committing = true;
    } else if (rec.type == CoordinatorLogRecordType::COMMITTED) {
      st.saw_committed = true;
    } else if (rec.type == CoordinatorLogRecordType::ABORTING) {
      st.saw_aborting = true;
    } else if (rec.type == CoordinatorLogRecordType::ABORTED) {
      st.saw_aborted = true;
    }
  });
  if (!rs.ok()) {
    return rs;
  }

  for (const auto& [txn_id, st] : states) {
    if (!st.saw_preparing || st.saw_committed || st.saw_aborted) {
      continue;
    }
    if (st.shards.empty()) {
      continue;
    }

    if (st.saw_committing) {
      Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTING, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      Status cs = DriveCommit(txn_id, st.shards);
      if (!cs.ok()) {
        return cs;
      }
      ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTED, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      continue;
    }

    if (st.saw_aborting) {
      Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      Status as = DriveAbort(txn_id, st.shards);
      if (!as.ok()) {
        return as;
      }
      ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      continue;
    }

    bool any_aborted = false;
    bool any_committed = false;
    bool all_prepared = true;
    for (uint32_t sid : st.shards) {
      TxnStateCode ss = QueryShardTxnState(sid, txn_id);
      if (ss == TxnStateCode::TXN_ABORTED) {
        any_aborted = true;
      }
      if (ss == TxnStateCode::TXN_COMMITTED) {
        any_committed = true;
      }
      if (ss != TxnStateCode::TXN_PREPARED && ss != TxnStateCode::TXN_COMMITTED) {
        all_prepared = false;
      }
    }

    if (any_aborted) {
      Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      Status as = DriveAbort(txn_id, st.shards);
      if (!as.ok()) {
        return as;
      }
      ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      continue;
    }

    if (all_prepared || any_committed) {
      Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTING, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      Status cs = DriveCommit(txn_id, st.shards);
      if (!cs.ok()) {
        return cs;
      }
      ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTED, txn_id);
      if (!ls.ok()) {
        return ls;
      }
      continue;
    }

    Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, txn_id);
    if (!ls.ok()) {
      return ls;
    }
    Status as = DriveAbort(txn_id, st.shards);
    if (!as.ok()) {
      return as;
    }
    ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, txn_id);
    if (!ls.ok()) {
      return ls;
    }
  }

  return Status::OK();
}

Status Coordinator::Read(uint64_t txn_id, uint32_t table_id, std::string_view key,
                         std::string* value) {
  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  const uint32_t shard_id = RouteShard(key);
  Status es = EnsureShardParticipant(*txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  ShardService::Stub* stub = GetStub(shard_id);
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  ExecuteRequest req;
  req.set_raft_txn_id(txn->raft_txn_id);
  req.set_op(OP_READ);
  req.set_table_id(table_id);
  req.set_key(key.data(), key.size());
  ExecuteResponse resp;
  const bool grpc_ok = stub->Execute(&ctx, req, &resp).ok();
  return FromExecuteResponse(grpc_ok, &resp, value);
}

Status Coordinator::Write(uint64_t txn_id, uint32_t table_id, std::string_view key,
                          std::string_view value) {
  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  const uint32_t shard_id = RouteShard(key);
  Status es = EnsureShardParticipant(*txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  ShardService::Stub* stub = GetStub(shard_id);
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  ExecuteRequest req;
  req.set_raft_txn_id(txn->raft_txn_id);
  req.set_op(OP_WRITE);
  req.set_table_id(table_id);
  req.set_key(key.data(), key.size());
  req.set_value(value.data(), value.size());
  ExecuteResponse resp;
  const bool grpc_ok = stub->Execute(&ctx, req, &resp).ok();
  return FromExecuteResponse(grpc_ok, &resp);
}

Status Coordinator::Delete(uint64_t txn_id, uint32_t table_id, std::string_view key) {
  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  const uint32_t shard_id = RouteShard(key);
  Status es = EnsureShardParticipant(*txn, shard_id);
  if (!es.ok()) {
    return es;
  }

  ShardService::Stub* stub = GetStub(shard_id);
  grpc::ClientContext ctx;
  ApplyDeadline(&ctx);
  ExecuteRequest req;
  req.set_raft_txn_id(txn->raft_txn_id);
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

  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }

  for (uint32_t sid = 0; sid < num_shards_; ++sid) {
    Status es = EnsureShardParticipant(*txn, sid);
    if (!es.ok()) {
      return es;
    }
  }


  std::vector<std::future<std::pair<Status, std::vector<std::pair<std::string, std::string>>>>>
      scan_futs;
  scan_futs.reserve(num_shards_);
  for (uint32_t sid = 0; sid < num_shards_; ++sid) {
    scan_futs.push_back(std::async(std::launch::async, [=, this]() {
      std::vector<std::pair<std::string, std::string>> shard_rows;
      ShardService::Stub* stub = GetStub(sid);
      if (!stub) {
        return std::make_pair(Status::Error(StatusCode::InvalidArgument, "unknown shard id"),
                              std::move(shard_rows));
      }
      grpc::ClientContext ctx;
      ApplyDeadline(&ctx);
      ExecuteRequest req;
      req.set_raft_txn_id(txn->raft_txn_id);
      req.set_op(OP_SCAN);
      req.set_table_id(table_id);
      req.set_range_start_pk(range_start_pk.data(), range_start_pk.size());
      req.set_range_end_exclusive(range_end_exclusive.data(), range_end_exclusive.size());
      req.set_range_end_open(range_end_open);
      ExecuteResponse resp;
      const bool grpc_ok = stub->Execute(&ctx, req, &resp).ok();
      if (!grpc_ok) {
        return std::make_pair(Status::IOError("gRPC Execute failed"), std::move(shard_rows));
      }
      if (!resp.ok()) {
        return std::make_pair(
            Status::Error(static_cast<StatusCode>(resp.error_code()), resp.error_message()),
            std::move(shard_rows));
      }
      shard_rows.reserve(static_cast<size_t>(resp.scan_rows_size()));
      for (const ScanRow& row : resp.scan_rows()) {
        shard_rows.emplace_back(row.pk(), row.row_value());
      }
      return std::make_pair(Status::OK(), std::move(shard_rows));
    }));
  }

  for (auto& fut : scan_futs) {
    auto [st, shard_rows] = fut.get();
    if (!st.ok()) {
      return st;
    }
    rows_out->insert(rows_out->end(), std::make_move_iterator(shard_rows.begin()),
                     std::make_move_iterator(shard_rows.end()));
  }

  std::sort(rows_out->begin(), rows_out->end(),
            [](const std::pair<std::string, std::string>& a,
               const std::pair<std::string, std::string>& b) { return a.first < b.first; });

  return Status::OK();
}

Status Coordinator::SingleShardCommit(uint64_t raft_txn_id, uint32_t shard_id,
                                     uint64_t commit_ts) {
  std::vector<uint32_t> parts{shard_id};
  Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::PREPARING, raft_txn_id, parts);
  if (!ls.ok()) {
    return ls;
  }
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
    Status abort_log = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, raft_txn_id);
    if (!abort_log.ok()) {
      return abort_log;
    }
    grpc::ClientContext abort_ctx;
    ApplyDeadline(&abort_ctx);
    AbortRequest ar;
    ar.set_raft_txn_id(raft_txn_id);
    AbortResponse aresp;
    (void)stub->Abort(&abort_ctx, ar, &aresp);
    (void)AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, raft_txn_id);
    return Status::Error(StatusCode::Conflict, presp.error_message());
  }

  ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTING, raft_txn_id);
  if (!ls.ok()) {
    return ls;
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
  return AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTED, raft_txn_id);
}

Status Coordinator::TwoPhaseCommit(uint64_t raft_txn_id, std::vector<uint32_t> parts,
                                   uint64_t commit_ts) {
  Status ls = AppendCoordinatorRecord(CoordinatorLogRecordType::PREPARING, raft_txn_id, parts);
  if (!ls.ok()) {
    return ls;
  }
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
    ls = AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, raft_txn_id);
    if (!ls.ok()) {
      return ls;
    }
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
    (void)AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, raft_txn_id);
    return Status::Error(StatusCode::Conflict, "2PC prepare failed");
  }

  ls = AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTING, raft_txn_id);
  if (!ls.ok()) {
    return ls;
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
  return AppendCoordinatorRecord(CoordinatorLogRecordType::COMMITTED, raft_txn_id);
}

Status Coordinator::Commit(uint64_t txn_id) {
  std::shared_ptr<CoordinatorTxn> txn;
  {
    std::scoped_lock lk(mu_);
    auto tit = txns_.find(txn_id);
    if (tit == txns_.end()) {
      return Status::Error(StatusCode::InvalidArgument, "unknown txn");
    }
    txn = tit->second;
  }
  if (txn->aborted) {
    std::scoped_lock lk(mu_);
    txns_.erase(txn_id);
    return Status::Error(StatusCode::InvalidArgument, "txn aborted");
  }
  if (txn->participant_shards.empty()) {
    std::scoped_lock lk(mu_);
    txns_.erase(txn_id);
    return Status::OK();
  }
  std::vector<uint32_t> parts(txn->participant_shards.begin(), txn->participant_shards.end());
  const uint64_t raft_txn_id = txn->raft_txn_id;
  const uint64_t commit_ts = ts_oracle_.Now();

  Status out;
  if (parts.size() == 1) {
    out = SingleShardCommit(raft_txn_id, parts.front(), commit_ts);
  } else {
    out = TwoPhaseCommit(raft_txn_id, std::move(parts), commit_ts);
  }
  {
    std::scoped_lock lk(mu_);
    txns_.erase(txn_id);
  }
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
    raft_tid = tit->second->raft_txn_id;
    parts.assign(tit->second->participant_shards.begin(), tit->second->participant_shards.end());
    txns_.erase(tit);
  }

  if (!parts.empty()) {
    (void)AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTING, raft_tid);
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
  if (!parts.empty()) {
    (void)AppendCoordinatorRecord(CoordinatorLogRecordType::ABORTED, raft_tid);
  }
  return Status::OK();
}

}  // namespace txndb
