#pragma once

#include "raft/raft_node.h"
#include "raft/raft_state_machine.h"
#include "raft/grpc_raft_transport.h"
#include "raft/raft_grpc_service.h"
#include "raft/raft_transport.h"
#include "shard.grpc.pb.h"
#include "storage/mvcc_store.h"
#include "txn/lock_manager.h"
#include "txn/txn_manager.h"
#include "txn/wal.h"

#include <grpcpp/grpcpp.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace txndb {

enum class ShardTransportMode { InProcess, Grpc };

struct ReplicaStack {
  std::unique_ptr<MVCCStore> store;
  std::unique_ptr<WAL> wal;
  std::unique_ptr<LockManager> lock_mgr;
  std::unique_ptr<TxnManager> txn_mgr;
  std::unique_ptr<RaftStateMachine> state_machine;
  std::unique_ptr<RaftNode> raft_node;
};

class ShardServiceImpl final : public ShardService::Service {
public:
  struct PendingTxn {
    uint64_t raft_txn_id{0};
    uint64_t snapshot_ts{0};
    uint64_t local_txn_id{0};
    bool prepared_replicated{false};
  };

  ShardServiceImpl(uint32_t shard_id, const std::string& data_dir, uint32_t num_replicas = 3,
                   ShardTransportMode mode = ShardTransportMode::InProcess);
  ShardServiceImpl(uint32_t shard_id, uint32_t replica_id, const std::string& data_dir,
                   GrpcRaftTransport* grpc_transport,
                   const std::unordered_map<uint32_t, std::string>& raft_peer_addrs);
  ~ShardServiceImpl();

  void Start();
  void Stop();

  grpc::Status Execute(grpc::ServerContext* context, const ExecuteRequest* request,
                       ExecuteResponse* response) override;

  grpc::Status Prepare(grpc::ServerContext* context, const PrepareRequest* request,
                       PrepareResponse* response) override;

  grpc::Status Commit(grpc::ServerContext* context, const CommitRequest* request,
                      CommitResponse* response) override;

  grpc::Status Abort(grpc::ServerContext* context, const AbortRequest* request,
                     AbortResponse* response) override;
  grpc::Status QueryTxnState(grpc::ServerContext* context, const TxnStateRequest* request,
                             TxnStateResponse* response) override;
  grpc::Status DistributedJoin(grpc::ServerContext* context, const DistributedJoinRequest* request,
                             DistributedJoinResponse* response) override;
  grpc::Status SetRaftPeerPartition(grpc::ServerContext* context,
                                    const SetRaftPeerPartitionRequest* request,
                                    SetRaftPeerPartitionResponse* response) override;

  MVCCStore* GetReplicaStore(uint32_t replica_id);
  TxnManager* GetReplicaTxnMgr(uint32_t replica_id);
  /// Leader's MVCC store (for tests).
  MVCCStore* GetLeaderStore();
  uint32_t GetLeaderReplicaId() const;
  uint32_t NumReplicas() const { return static_cast<uint32_t>(replicas_.size()); }
  void DisconnectReplicaForTest(uint32_t replica_id);
  void ReconnectReplicaForTest(uint32_t replica_id);
  void SetRaftPeerPartitionForTest(uint32_t peer_replica_id, bool partitioned);

  RaftNode* GetRaftNode(uint32_t replica_id);

private:
  ReplicaStack* FindLeader();
  bool ProposeAndWait(RaftEntryType type, std::string payload,
                      std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));

  void InitReplica(uint32_t replica_index, uint32_t raft_node_id, std::vector<uint32_t> peers,
                   RaftTransport* transport, const std::string& replica_data_path);

  uint32_t shard_id_{0};
  uint32_t local_replica_id_{0};
  std::string data_dir_;
  bool started_{false};
  ShardTransportMode mode_{ShardTransportMode::InProcess};

  InProcessTransport inproc_transport_;
  GrpcRaftTransport* grpc_transport_{nullptr};
  std::vector<std::unique_ptr<ReplicaStack>> replicas_;
  std::mutex pending_mu_;
  std::unordered_map<uint64_t, PendingTxn> pending_txns_;
  std::unordered_map<uint64_t, TxnStateCode> terminal_txn_states_;
};

class ShardServer {
public:
  ShardServer(uint32_t shard_id, const std::string& data_dir, const std::string& listen_addr,
              uint32_t num_replicas = 3,
              ShardTransportMode mode = ShardTransportMode::InProcess);
  ~ShardServer();

  void Start();
  void Stop();
  void Wait();

  ShardServiceImpl* GetService() { return &service_; }

private:
  ShardServiceImpl service_;
  std::string listen_addr_;
  std::unique_ptr<grpc::Server> server_;
};

class ReplicaServer {
public:
  ReplicaServer(uint32_t shard_id, uint32_t replica_id, const std::string& data_dir,
                const std::string& shard_listen_addr, const std::string& raft_listen_addr,
                const std::unordered_map<uint32_t, std::string>& raft_peer_addrs);
  ~ReplicaServer();

  void Start();
  void Stop();
  void Wait();

  ShardServiceImpl* GetShardService() { return &shard_service_; }
  RaftNode* GetRaftNode() { return shard_service_.GetRaftNode(0); }

private:
  GrpcRaftTransport raft_transport_;
  ShardServiceImpl shard_service_;
  RaftServiceImpl raft_service_;
  std::string shard_listen_addr_;
  std::string raft_listen_addr_;
  std::unique_ptr<grpc::Server> shard_server_;
  std::unique_ptr<grpc::Server> raft_server_;
};

}  // namespace txndb
