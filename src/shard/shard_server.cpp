#include "shard/shard_server.h"

#include "shard/distributed_join_handler.h"
#include "storage/status.h"

#include <filesystem>
#include <stdexcept>
#include <sstream>
#include <thread>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace txndb {

namespace {

std::string JoinPath(std::string_view a, std::string_view b) {
  fs::path p(a);
  p /= fs::path(b);
  return p.string();
}

uint32_t ErrorCode(StatusCode c) { return static_cast<uint32_t>(c); }

}  // namespace

void ShardServiceImpl::InitReplica(uint32_t replica_index, uint32_t raft_node_id,
                                   std::vector<uint32_t> peers, RaftTransport* transport,
                                   const std::string& replica_data_path) {
  std::error_code ec;
  fs::create_directories(replica_data_path, ec);

  auto stack = std::make_unique<ReplicaStack>();
  const std::string rock = JoinPath(replica_data_path, "db");
  const std::string walp = JoinPath(replica_data_path, "wal");
  const std::string raft_dir = JoinPath(replica_data_path, "raft");
  fs::create_directories(rock, ec);

  if (!MVCCStore::Open(rock, &stack->store).ok()) {
    throw std::runtime_error("MVCCStore::Open failed");
  }
  if (!WAL::Open(walp, &stack->wal).ok()) {
    throw std::runtime_error("WAL::Open failed");
  }
  stack->lock_mgr = std::make_unique<LockManager>();
  stack->txn_mgr =
      std::make_unique<TxnManager>(stack->store.get(), stack->wal.get(), stack->lock_mgr.get());
  if (!stack->txn_mgr->Recover().ok()) {
    throw std::runtime_error("TxnManager::Recover failed");
  }
  stack->state_machine = std::make_unique<RaftStateMachine>(stack->txn_mgr.get());

  stack->raft_node = std::make_unique<RaftNode>(raft_node_id, std::move(peers), transport,
                                                stack->state_machine->WrapApply(), raft_dir);
  if (mode_ == ShardTransportMode::InProcess) {
    inproc_transport_.RegisterNode(raft_node_id, stack->raft_node.get());
  }

  if (replicas_.size() <= replica_index) {
    replicas_.resize(replica_index + 1);
  }
  replicas_[replica_index] = std::move(stack);
}

ShardServiceImpl::ShardServiceImpl(uint32_t shard_id, const std::string& data_dir,
                                   uint32_t num_replicas, ShardTransportMode mode)
    : shard_id_(shard_id), data_dir_(data_dir), mode_(mode) {
  std::error_code ec;
  fs::create_directories(data_dir_, ec);

  if (mode_ != ShardTransportMode::InProcess) {
    throw std::invalid_argument("multi-replica ShardServiceImpl requires InProcess mode");
  }

  replicas_.reserve(num_replicas);
  std::ostringstream prefix;
  prefix << data_dir << "/shard" << shard_id << "_rep";

  for (uint32_t i = 0; i < num_replicas; ++i) {
    std::vector<uint32_t> peers;
    for (uint32_t j = 0; j < num_replicas; ++j) {
      if (j != i) {
        peers.push_back(j);
      }
    }
    const std::string base = prefix.str() + std::to_string(i);
    InitReplica(i, i, std::move(peers), &inproc_transport_, base);
  }
}

ShardServiceImpl::ShardServiceImpl(uint32_t shard_id, uint32_t replica_id,
                                   const std::string& data_dir, GrpcRaftTransport* grpc_transport,
                                   const std::unordered_map<uint32_t, std::string>& raft_peer_addrs)
    : shard_id_(shard_id),
      local_replica_id_(replica_id),
      data_dir_(data_dir),
      mode_(ShardTransportMode::Grpc),
      grpc_transport_(grpc_transport) {
  if (!grpc_transport_) {
    throw std::invalid_argument("grpc transport required");
  }
  grpc_transport_->SetLocalNodeId(replica_id);

  std::vector<uint32_t> peers;
  peers.reserve(raft_peer_addrs.size());
  for (const auto& [peer_id, addr] : raft_peer_addrs) {
    if (peer_id != replica_id) {
      peers.push_back(peer_id);
      grpc_transport_->AddPeer(peer_id, addr);
    }
  }

  InitReplica(0, replica_id, std::move(peers), grpc_transport_, data_dir_);
}

ShardServiceImpl::~ShardServiceImpl() { Stop(); }

void ShardServiceImpl::Start() {
  if (started_) {
    return;
  }
  for (auto& r : replicas_) {
    r->raft_node->Start();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  started_ = true;
}

void ShardServiceImpl::Stop() {
  if (!started_) {
    return;
  }
  for (auto& r : replicas_) {
    if (r->raft_node) {
      r->raft_node->Stop();
    }
  }
  started_ = false;
}

ReplicaStack* ShardServiceImpl::FindLeader() {
  for (auto& r : replicas_) {
    if (r->raft_node && r->raft_node->GetRole() == RaftRole::LEADER) {
      return r.get();
    }
  }
  return nullptr;
}

bool ShardServiceImpl::ProposeAndWait(RaftEntryType type, std::string payload,
                                      std::chrono::milliseconds timeout) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    return false;
  }
  const uint64_t ix = L->raft_node->Propose(type, std::move(payload));
  if (ix == 0) {
    return false;
  }
  return L->raft_node->WaitForCommit(ix, timeout);
}

MVCCStore* ShardServiceImpl::GetReplicaStore(uint32_t replica_id) {
  if (replica_id >= replicas_.size()) {
    return nullptr;
  }
  return replicas_[replica_id]->store.get();
}

TxnManager* ShardServiceImpl::GetReplicaTxnMgr(uint32_t replica_id) {
  if (replica_id >= replicas_.size()) {
    return nullptr;
  }
  return replicas_[replica_id]->txn_mgr.get();
}

MVCCStore* ShardServiceImpl::GetLeaderStore() {
  ReplicaStack* L = FindLeader();
  return L ? L->store.get() : nullptr;
}

uint32_t ShardServiceImpl::GetLeaderReplicaId() const {
  for (uint32_t i = 0; i < replicas_.size(); ++i) {
    if (replicas_[i]->raft_node && replicas_[i]->raft_node->GetRole() == RaftRole::LEADER) {
      return i;
    }
  }
  return UINT32_MAX;
}

void ShardServiceImpl::DisconnectReplicaForTest(uint32_t replica_id) {
  if (mode_ == ShardTransportMode::InProcess) {
    inproc_transport_.DisconnectNode(replica_id);
    return;
  }
  if (grpc_transport_) {
    grpc_transport_->DisconnectPeer(replica_id);
  }
}

void ShardServiceImpl::ReconnectReplicaForTest(uint32_t replica_id) {
  if (mode_ == ShardTransportMode::InProcess) {
    if (replica_id >= replicas_.size()) {
      return;
    }
    inproc_transport_.ReconnectNode(replica_id, replicas_[replica_id]->raft_node.get());
    return;
  }
  if (grpc_transport_) {
    grpc_transport_->ReconnectPeer(replica_id);
  }
}

void ShardServiceImpl::SetRaftPeerPartitionForTest(uint32_t peer_replica_id, bool partitioned) {
  if (partitioned) {
    DisconnectReplicaForTest(peer_replica_id);
  } else {
    ReconnectReplicaForTest(peer_replica_id);
  }
}

RaftNode* ShardServiceImpl::GetRaftNode(uint32_t replica_id) {
  if (replica_id >= replicas_.size() || !replicas_[replica_id]) {
    return nullptr;
  }
  return replicas_[replica_id]->raft_node.get();
}

grpc::Status ShardServiceImpl::SetRaftPeerPartition(
    grpc::ServerContext* /*context*/, const SetRaftPeerPartitionRequest* request,
    SetRaftPeerPartitionResponse* response) {
  SetRaftPeerPartitionForTest(request->peer_replica_id(), request->partitioned());
  response->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status ShardServiceImpl::Execute(grpc::ServerContext* /*context*/, const ExecuteRequest* request,
                                       ExecuteResponse* response) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    response->set_ok(false);
    response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
    response->set_error_message("no raft leader available");
    return grpc::Status::OK;
  }

  const uint64_t raft_txn_id = request->raft_txn_id();
  switch (static_cast<int>(request->op())) {
    case 0: /* OP_BEGIN */ {
      const uint64_t local = L->txn_mgr->Begin(request->snapshot_ts());
      L->state_machine->RegisterLocalTxn(raft_txn_id, local);
      {
        std::scoped_lock lk(pending_mu_);
        pending_txns_[raft_txn_id] =
            PendingTxn{raft_txn_id, request->snapshot_ts(), local, false};
      }
      response->set_ok(true);
      return grpc::Status::OK;
    }
    case 1: /* OP_READ */ {
      uint64_t local = 0;
      {
        std::scoped_lock lk(pending_mu_);
        auto it = pending_txns_.find(raft_txn_id);
        if (it != pending_txns_.end()) {
          local = it->second.local_txn_id;
        }
      }
      if (local == 0) {
        local = L->state_machine->GetLocalTxnId(raft_txn_id);
      }
      if (local == 0) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
        response->set_error_message("txn not begun on shard");
        return grpc::Status::OK;
      }
      std::string val;
      const Status rs = L->txn_mgr->Read(local, request->table_id(), request->key(), &val);
      if (!rs.ok()) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(rs.code()));
        response->set_error_message(rs.message());
        return grpc::Status::OK;
      }
      response->set_ok(true);
      response->set_value(std::move(val));
      return grpc::Status::OK;
    }
    case 2: /* OP_WRITE */ {
      uint64_t local = 0;
      {
        std::scoped_lock lk(pending_mu_);
        auto it = pending_txns_.find(raft_txn_id);
        if (it != pending_txns_.end()) {
          local = it->second.local_txn_id;
        }
      }
      if (local == 0) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
        response->set_error_message("txn not begun on shard");
        return grpc::Status::OK;
      }
      const Status ws =
          L->txn_mgr->Write(local, request->table_id(), request->key(), request->value());
      if (!ws.ok()) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(ws.code()));
        response->set_error_message(ws.message());
        return grpc::Status::OK;
      }
      response->set_ok(true);
      return grpc::Status::OK;
    }
    case 3: /* OP_DELETE */ {
      uint64_t local = 0;
      {
        std::scoped_lock lk(pending_mu_);
        auto it = pending_txns_.find(raft_txn_id);
        if (it != pending_txns_.end()) {
          local = it->second.local_txn_id;
        }
      }
      if (local == 0) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
        response->set_error_message("txn not begun on shard");
        return grpc::Status::OK;
      }
      const Status ds = L->txn_mgr->Delete(local, request->table_id(), request->key());
      if (!ds.ok()) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(ds.code()));
        response->set_error_message(ds.message());
        return grpc::Status::OK;
      }
      response->set_ok(true);
      return grpc::Status::OK;
    }
    case 4: /* OP_SCAN */ {
      uint64_t local = 0;
      {
        std::scoped_lock lk(pending_mu_);
        auto it = pending_txns_.find(raft_txn_id);
        if (it != pending_txns_.end()) {
          local = it->second.local_txn_id;
        }
      }
      if (local == 0) {
        local = L->state_machine->GetLocalTxnId(raft_txn_id);
      }
      if (local == 0) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
        response->set_error_message("txn not begun on shard");
        return grpc::Status::OK;
      }
      std::vector<std::pair<std::string, std::string>> rows;
      const Status rs = L->txn_mgr->Scan(local, request->table_id(), request->range_start_pk(),
                                         request->range_end_exclusive(), request->range_end_open(),
                                         &rows);
      if (!rs.ok()) {
        response->set_ok(false);
        response->set_error_code(ErrorCode(rs.code()));
        response->set_error_message(rs.message());
        return grpc::Status::OK;
      }
      response->set_ok(true);
      for (auto& pr : rows) {
        ScanRow* sr = response->add_scan_rows();
        sr->set_pk(std::move(pr.first));
        sr->set_row_value(std::move(pr.second));
      }
      return grpc::Status::OK;
    }
    default:
      response->set_ok(false);
      response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
      response->set_error_message("unknown op");
      return grpc::Status::OK;
  }
}

grpc::Status ShardServiceImpl::Prepare(grpc::ServerContext* /*context*/, const PrepareRequest* request,
                                       PrepareResponse* response) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    response->set_vote_commit(false);
    response->set_error_message("no leader");
    return grpc::Status::OK;
  }

  PendingTxn p;
  {
    std::scoped_lock lk(pending_mu_);
    auto it = pending_txns_.find(request->raft_txn_id());
    if (it == pending_txns_.end()) {
      response->set_vote_commit(false);
      response->set_error_message("unknown txn");
      return grpc::Status::OK;
    }
    p = it->second;
  }

  const Status local_prepare = L->txn_mgr->Prepare(p.local_txn_id, request->commit_ts());
  if (!local_prepare.ok()) {
    (void)L->txn_mgr->Abort(p.local_txn_id);
    L->state_machine->ForgetTxn(request->raft_txn_id());
    std::scoped_lock lk(pending_mu_);
    pending_txns_.erase(request->raft_txn_id());
    response->set_vote_commit(false);
    response->set_error_message(local_prepare.message());
    return grpc::Status::OK;
  }

  Transaction* txn = L->txn_mgr->GetTxn(p.local_txn_id);
  if (!txn) {
    L->state_machine->ForgetTxn(request->raft_txn_id());
    std::scoped_lock lk(pending_mu_);
    pending_txns_.erase(request->raft_txn_id());
    response->set_vote_commit(false);
    response->set_error_message("prepared txn missing");
    return grpc::Status::OK;
  }

  std::vector<RaftStateMachine::PrepareBatchWrite> writes;
  writes.reserve(txn->write_set.size());
  for (const auto& w : txn->write_set) {
    RaftStateMachine::PrepareBatchWrite bw;
    bw.is_delete = w.is_delete;
    bw.table_id = w.table_id;
    bw.key = w.key;
    bw.value = w.value;
    writes.push_back(std::move(bw));
  }

  const std::string payload = RaftStateMachine::PackPrepareBatchPayload(
      request->raft_txn_id(), p.snapshot_ts, request->commit_ts(), writes);
  if (!ProposeAndWait(RaftEntryType::TXN_PREPARE_BATCH, payload)) {
    (void)L->txn_mgr->Abort(p.local_txn_id);
    L->state_machine->ForgetTxn(request->raft_txn_id());
    std::scoped_lock lk(pending_mu_);
    pending_txns_.erase(request->raft_txn_id());
    response->set_vote_commit(false);
    response->set_error_message("prepare batch not replicated");
    return grpc::Status::OK;
  }

  auto pr = L->state_machine->TakePrepareResult(request->raft_txn_id());
  if (!pr.has_value() || !pr->success) {
    response->set_vote_commit(false);
    response->set_error_message(pr ? pr->error : "missing prepare result");
    return grpc::Status::OK;
  }

  {
    std::scoped_lock lk(pending_mu_);
    auto it = pending_txns_.find(request->raft_txn_id());
    if (it != pending_txns_.end()) {
      it->second.prepared_replicated = true;
    }
  }
  response->set_vote_commit(true);
  response->set_error_message("");
  return grpc::Status::OK;
}

grpc::Status ShardServiceImpl::Commit(grpc::ServerContext* /*context*/, const CommitRequest* request,
                                      CommitResponse* response) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    response->set_ok(false);
    response->set_error_message("no leader");
    return grpc::Status::OK;
  }
  auto payload = RaftStateMachine::PackTxnPayload(WAL::SerializeCommitPayload(request->commit_ts()),
                                                  request->raft_txn_id());
  const bool ok = ProposeAndWait(RaftEntryType::TXN_COMMIT, std::move(payload));
  response->set_ok(ok);
  if (ok) {
    std::scoped_lock lk(pending_mu_);
    pending_txns_.erase(request->raft_txn_id());
    terminal_txn_states_[request->raft_txn_id()] = TxnStateCode::TXN_COMMITTED;
  }
  return grpc::Status::OK;
}

grpc::Status ShardServiceImpl::Abort(grpc::ServerContext* /*context*/, const AbortRequest* request,
                                     AbortResponse* response) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    response->set_ok(true);
    return grpc::Status::OK;
  }

  PendingTxn p;
  bool has_pending = false;
  {
    std::scoped_lock lk(pending_mu_);
    auto it = pending_txns_.find(request->raft_txn_id());
    if (it != pending_txns_.end()) {
      has_pending = true;
      p = it->second;
    }
  }

  if (has_pending && !p.prepared_replicated) {
    (void)L->txn_mgr->Abort(p.local_txn_id);
    L->state_machine->ForgetTxn(request->raft_txn_id());
    std::scoped_lock lk(pending_mu_);
    pending_txns_.erase(request->raft_txn_id());
    terminal_txn_states_[request->raft_txn_id()] = TxnStateCode::TXN_ABORTED;
    response->set_ok(true);
    return grpc::Status::OK;
  }

  const auto payload = RaftStateMachine::PackAbortPayload(request->raft_txn_id());
  const bool ok = ProposeAndWait(RaftEntryType::TXN_ABORT, std::string(payload));
  if (ok) {
    std::scoped_lock lk(pending_mu_);
    pending_txns_.erase(request->raft_txn_id());
    terminal_txn_states_[request->raft_txn_id()] = TxnStateCode::TXN_ABORTED;
  }
  response->set_ok(ok);
  return grpc::Status::OK;
}

grpc::Status ShardServiceImpl::DistributedJoin(grpc::ServerContext* /*context*/,
                                               const DistributedJoinRequest* request,
                                               DistributedJoinResponse* response) {
  ReplicaStack* L = FindLeader();
  if (!L) {
    response->set_ok(false);
    response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
    response->set_error_message("no raft leader available");
    return grpc::Status::OK;
  }

  uint64_t local = 0;
  {
    std::scoped_lock lk(pending_mu_);
    auto it = pending_txns_.find(request->raft_txn_id());
    if (it != pending_txns_.end()) {
      local = it->second.local_txn_id;
    }
  }
  if (local == 0) {
    local = L->state_machine->GetLocalTxnId(request->raft_txn_id());
  }
  if (local == 0) {
    response->set_ok(false);
    response->set_error_code(ErrorCode(StatusCode::InvalidArgument));
    response->set_error_message("txn not begun on shard");
    return grpc::Status::OK;
  }

  (void)RunDistributedJoin(L->txn_mgr.get(), local, *request, response);
  return grpc::Status::OK;
}

grpc::Status ShardServiceImpl::QueryTxnState(grpc::ServerContext* /*context*/,
                                             const TxnStateRequest* request,
                                             TxnStateResponse* response) {
  std::scoped_lock lk(pending_mu_);
  auto t_it = terminal_txn_states_.find(request->raft_txn_id());
  if (t_it != terminal_txn_states_.end()) {
    response->set_ok(true);
    response->set_state(t_it->second);
    return grpc::Status::OK;
  }

  auto p_it = pending_txns_.find(request->raft_txn_id());
  if (p_it != pending_txns_.end()) {
    response->set_ok(true);
    response->set_state(p_it->second.prepared_replicated ? TxnStateCode::TXN_PREPARED
                                                         : TxnStateCode::TXN_IN_FLIGHT);
    return grpc::Status::OK;
  }

  if (!replicas_.empty() && replicas_[0] && replicas_[0]->state_machine &&
      replicas_[0]->state_machine->IsTxnPrepared(request->raft_txn_id())) {
    response->set_ok(true);
    response->set_state(TxnStateCode::TXN_PREPARED);
    return grpc::Status::OK;
  }

  response->set_ok(true);
  response->set_state(TxnStateCode::TXN_UNKNOWN);
  return grpc::Status::OK;
}

ShardServer::ShardServer(uint32_t shard_id, const std::string& data_dir,
                         const std::string& listen_addr, uint32_t num_replicas,
                         ShardTransportMode mode)
    : service_(shard_id, data_dir, num_replicas, mode), listen_addr_(listen_addr) {}

ReplicaServer::ReplicaServer(uint32_t shard_id, uint32_t replica_id, const std::string& data_dir,
                             const std::string& shard_listen_addr,
                             const std::string& raft_listen_addr,
                             const std::unordered_map<uint32_t, std::string>& raft_peer_addrs)
    : shard_service_(shard_id, replica_id, data_dir, &raft_transport_, raft_peer_addrs),
      raft_service_(shard_service_.GetRaftNode(0)),
      shard_listen_addr_(shard_listen_addr),
      raft_listen_addr_(raft_listen_addr) {}

ReplicaServer::~ReplicaServer() { Stop(); }

void ReplicaServer::Start() {
  shard_service_.Start();
  grpc::ServerBuilder raft_builder;
  raft_builder.AddListeningPort(raft_listen_addr_, grpc::InsecureServerCredentials());
  raft_builder.RegisterService(&raft_service_);
  raft_server_ = raft_builder.BuildAndStart();

  grpc::ServerBuilder shard_builder;
  shard_builder.AddListeningPort(shard_listen_addr_, grpc::InsecureServerCredentials());
  shard_builder.RegisterService(&shard_service_);
  shard_server_ = shard_builder.BuildAndStart();
}

void ReplicaServer::Stop() {
  if (shard_server_) {
    shard_server_->Shutdown();
    shard_server_->Wait();
    shard_server_.reset();
  }
  if (raft_server_) {
    raft_server_->Shutdown();
    raft_server_->Wait();
    raft_server_.reset();
  }
  shard_service_.Stop();
}

void ReplicaServer::Wait() {
  if (shard_server_) {
    shard_server_->Wait();
  }
}

ShardServer::~ShardServer() { Stop(); }

void ShardServer::Start() {
  service_.Start();
  grpc::ServerBuilder builder;
  builder.AddListeningPort(listen_addr_, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);
  server_ = builder.BuildAndStart();
}

void ShardServer::Stop() {
  if (server_) {
    server_->Shutdown();
    server_->Wait();
    server_.reset();
  }
  service_.Stop();
}

void ShardServer::Wait() {
  if (server_) {
    server_->Wait();
  }
}

}  // namespace txndb
