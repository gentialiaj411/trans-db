#pragma once

#include "raft/raft_log.h"
#include "storage/status.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace txndb {

enum class RaftRole : uint8_t { FOLLOWER, CANDIDATE, LEADER };

struct AppendEntriesRequest {
  uint64_t term = 0;
  uint32_t leader_id = 0;
  uint64_t prev_log_index = 0;
  uint64_t prev_log_term = 0;
  std::vector<RaftLogEntry> entries;
  uint64_t leader_commit = 0;
};

struct AppendEntriesResponse {
  uint64_t term = 0;
  bool success = false;
  uint64_t match_index = 0;
};

struct RequestVoteRequest {
  uint64_t term = 0;
  uint32_t candidate_id = 0;
  uint64_t last_log_index = 0;
  uint64_t last_log_term = 0;
};

struct RequestVoteResponse {
  uint64_t term = 0;
  bool vote_granted = false;
};

using ApplyCallback = std::function<void(const RaftLogEntry& entry)>;

class RaftTransport {
public:
  virtual ~RaftTransport() = default;
  virtual AppendEntriesResponse SendAppendEntries(uint32_t target_id,
                                                  const AppendEntriesRequest& req) = 0;
  virtual RequestVoteResponse SendRequestVote(uint32_t target_id,
                                              const RequestVoteRequest& req) = 0;
};

class RaftNode {
public:
  RaftNode(uint32_t node_id, std::vector<uint32_t> peer_ids, RaftTransport* transport,
           ApplyCallback apply_cb);

  ~RaftNode();

  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  uint64_t Propose(RaftEntryType type, std::string payload);

  void Start();

  void Stop();

  AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& req);
  RequestVoteResponse HandleRequestVote(const RequestVoteRequest& req);

  RaftRole GetRole() const;
  uint64_t GetCurrentTerm() const;
  uint32_t GetLeaderId() const;
  uint64_t GetCommitIndex() const;
  uint64_t GetLastApplied() const;
  const RaftLog& GetLog() const;
  uint32_t GetNodeId() const { return node_id_; }

  bool WaitForCommit(uint64_t index, std::chrono::milliseconds timeout);

private:
  void TickLoop();
  void TickOnce();
  void BecomeFollower(uint64_t term);
  void BecomeCandidateLocked();
  void BecomeLeaderLocked();
  void StartElection();
  void SendAppendEntriesToAll();
  void ProcessAppendEntriesResponse(uint32_t peer_id, const AppendEntriesResponse& resp,
                                    uint64_t sent_term);
  void AdvanceCommitIndexLocked();
  void ApplyCommitted();
  void RandomizeElectionTimeoutLocked();
  int MajorityThreshold() const;

  uint64_t current_term_{0};
  std::optional<uint32_t> voted_for_;
  RaftLog log_;

  RaftRole role_{RaftRole::FOLLOWER};
  uint32_t leader_id_{UINT32_MAX};
  uint64_t commit_index_{0};
  uint64_t last_applied_{0};

  std::unordered_map<uint32_t, uint64_t> next_index_;
  std::unordered_map<uint32_t, uint64_t> match_index_;

  uint32_t node_id_;
  std::vector<uint32_t> peer_ids_;
  RaftTransport* transport_{nullptr};
  ApplyCallback apply_cb_;

  std::chrono::steady_clock::time_point last_heartbeat_recv_;
  std::chrono::steady_clock::time_point last_broadcast_;
  std::chrono::milliseconds election_timeout_{250};
  std::mt19937 rng_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::atomic<bool> running_{false};
  std::unique_ptr<std::thread> tick_thread_;

  static constexpr auto kHeartbeatInterval = std::chrono::milliseconds(50);
  static constexpr auto kElectionTimeoutMin = std::chrono::milliseconds(150);
  static constexpr auto kElectionTimeoutMax = std::chrono::milliseconds(300);
  static constexpr auto kTickInterval = std::chrono::milliseconds(10);
};

}  // namespace txndb
