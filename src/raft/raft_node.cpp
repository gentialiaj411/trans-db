#include "raft/raft_node.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <thread>

namespace txndb {

RaftNode::RaftNode(uint32_t node_id, std::vector<uint32_t> peer_ids, RaftTransport* transport,
                   ApplyCallback apply_cb)
    : RaftNode(node_id, std::move(peer_ids), transport, std::move(apply_cb), "") {}

RaftNode::RaftNode(uint32_t node_id, std::vector<uint32_t> peer_ids, RaftTransport* transport,
                   ApplyCallback apply_cb, std::string raft_dir)
    : node_id_(node_id),
      peer_ids_(std::move(peer_ids)),
      transport_(transport),
      apply_cb_(std::move(apply_cb)),
      raft_dir_(std::move(raft_dir)),
      meta_path_(raft_dir_.empty() ? "" : (raft_dir_ + "/raft_meta")),
      log_(raft_dir_.empty() ? std::string() : (raft_dir_ + "/raft_log")),
      rng_(std::random_device{}()) {
  if (!raft_dir_.empty()) {
    std::error_code ec;
    std::filesystem::create_directories(raft_dir_, ec);
    LoadMeta();
  }
  last_heartbeat_recv_ = std::chrono::steady_clock::now();
  last_broadcast_ = last_heartbeat_recv_;
  RandomizeElectionTimeoutLocked();
}

RaftNode::~RaftNode() { Stop(); }

int RaftNode::MajorityThreshold() const {
  const int sz = static_cast<int>(peer_ids_.size()) + 1;
  return sz / 2 + 1;
}

void RaftNode::RandomizeElectionTimeoutLocked() {
  std::uniform_int_distribution<int> dist(static_cast<int>(kElectionTimeoutMin.count()),
                                           static_cast<int>(kElectionTimeoutMax.count()));
  election_timeout_ = std::chrono::milliseconds(dist(rng_));
}

void RaftNode::BecomeFollower(uint64_t term) {
  current_term_ = term;
  voted_for_.reset();
  PersistMeta();
  role_ = RaftRole::FOLLOWER;
  leader_id_ = UINT32_MAX;
  RandomizeElectionTimeoutLocked();
  last_heartbeat_recv_ = std::chrono::steady_clock::now();
}

void RaftNode::BecomeCandidateLocked() {
  role_ = RaftRole::CANDIDATE;
  current_term_++;
  voted_for_ = node_id_;
  PersistMeta();
  leader_id_ = UINT32_MAX;
  last_heartbeat_recv_ = std::chrono::steady_clock::now();
  RandomizeElectionTimeoutLocked();
}

void RaftNode::BecomeLeaderLocked() {
  role_ = RaftRole::LEADER;
  leader_id_ = node_id_;
  for (uint32_t p : peer_ids_) {
    next_index_[p] = log_.LastIndex() + 1;
    match_index_[p] = 0;
  }
  last_broadcast_ = std::chrono::steady_clock::time_point{};
  immediate_replicate_ = true;
  log_.Append(current_term_, RaftEntryType::NOOP, "");
}

void RaftNode::StartElection() {
  uint64_t term_sent = 0;
  uint32_t self = 0;
  uint64_t lli = 0;
  uint64_t llterm = 0;
  std::vector<uint32_t> peers;
  {
    std::scoped_lock lk(mu_);
    if (!running_ || role_ == RaftRole::LEADER) {
      return;
    }
    BecomeCandidateLocked();
    term_sent = current_term_;
    self = node_id_;
    lli = log_.LastIndex();
    llterm = log_.TermAt(lli);
    peers = peer_ids_;
  }

  int votes = 1;
  for (uint32_t p : peers) {
    RequestVoteRequest req{term_sent, self, lli, llterm};
    RequestVoteResponse resp = transport_->SendRequestVote(p, req);

    std::unique_lock lk(mu_);
    if (!running_) {
      return;
    }
    if (resp.term != 0 && resp.term > current_term_) {
      BecomeFollower(resp.term);
      return;
    }
    if (current_term_ != term_sent || role_ != RaftRole::CANDIDATE) {
      return;
    }
    if (resp.term != 0 && resp.term < term_sent) {
      continue;
    }
    if (resp.term == 0 && !resp.vote_granted) {
      continue;
    }
    if (resp.vote_granted) {
      votes++;
    }
    if (votes >= MajorityThreshold()) {
      BecomeLeaderLocked();
      lk.unlock();
      SendAppendEntriesToAll();
      return;
    }
  }
}

void RaftNode::ProcessAppendEntriesResponse(uint32_t peer_id, const AppendEntriesResponse& resp,
                                            uint64_t sent_term) {
  std::unique_lock lk(mu_);
  if (!running_) {
    return;
  }
  if (resp.term != 0 && resp.term > current_term_) {
    BecomeFollower(resp.term);
    return;
  }
  if (sent_term != current_term_ || role_ != RaftRole::LEADER) {
    return;
  }
  if (!resp.success) {
    next_index_[peer_id] = std::max<uint64_t>(1, next_index_[peer_id] - 1);
    return;
  }
  match_index_[peer_id] = std::max(match_index_[peer_id], resp.match_index);
  next_index_[peer_id] = std::max(next_index_[peer_id], resp.match_index + 1);

  uint64_t before = commit_index_;
  AdvanceCommitIndexLocked();
  if (commit_index_ > before) {
    cv_.notify_all();
  }

  lk.unlock();
  ApplyCommitted();
}

void RaftNode::AdvanceCommitIndexLocked() {
  if (role_ != RaftRole::LEADER) {
    return;
  }
  const uint64_t last = log_.LastIndex();
  if (commit_index_ >= last) {
    return;
  }
  for (uint64_t n = last; n > commit_index_; --n) {
    if (log_.TermAt(n) != current_term_) {
      continue;
    }
    int votes = 1;
    for (uint32_t p : peer_ids_) {
      if (match_index_.count(p) && match_index_[p] >= n) {
        votes++;
      }
    }
    if (votes >= MajorityThreshold()) {
      commit_index_ = n;
      cv_.notify_all();
      return;
    }
  }
}

void RaftNode::SendAppendEntriesToAll() {
  struct Job {
    uint32_t peer = 0;
    AppendEntriesRequest req;
  };
  std::vector<Job> jobs;
  uint64_t sent_term = 0;

  {
    std::scoped_lock lk(mu_);
    if (role_ != RaftRole::LEADER || !running_) {
      return;
    }
    last_broadcast_ = std::chrono::steady_clock::now();
    sent_term = current_term_;
    const uint32_t lid = node_id_;
    const uint64_t lc = commit_index_;
    jobs.reserve(peer_ids_.size());
    for (uint32_t p : peer_ids_) {
      AppendEntriesRequest r;
      r.term = sent_term;
      r.leader_id = lid;
      const uint64_t ni = next_index_[p];
      const uint64_t prev = ni - 1;
      r.prev_log_index = prev;
      r.prev_log_term = (prev == 0) ? 0 : log_.TermAt(prev);
      const uint64_t last = log_.LastIndex();
      if (ni <= last) {
        r.entries = log_.GetRange(ni, last);
      }
      r.leader_commit = lc;
      jobs.push_back(Job{p, std::move(r)});
    }
  }

  for (Job& job : jobs) {
    AppendEntriesResponse resp =
        transport_->SendAppendEntries(job.peer, job.req);
    ProcessAppendEntriesResponse(job.peer, resp, sent_term);
  }
}

void RaftNode::ApplyCommitted() {
  std::scoped_lock lk(mu_);
  while (last_applied_ < commit_index_) {
    const uint64_t next_idx = last_applied_ + 1;
    auto e = log_.Get(next_idx);
    if (!e) {
      break;
    }
    if (apply_cb_) {
      apply_cb_(*e);
    }
    last_applied_ = next_idx;
    cv_.notify_all();
  }
}

void RaftNode::TickOnce() {
  ApplyCommitted();
  std::unique_lock lk(mu_);
  if (!running_) {
    return;
  }
  const auto now = std::chrono::steady_clock::now();
  if (role_ == RaftRole::LEADER) {
    const bool heartbeat_due =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - last_broadcast_) >=
        kHeartbeatInterval;
    const bool should_send = immediate_replicate_ || heartbeat_due;
    immediate_replicate_ = false;
    lk.unlock();
    if (should_send) {
      SendAppendEntriesToAll();
    }
    lk.lock();
    if (running_ && role_ == RaftRole::LEADER) {
      AdvanceCommitIndexLocked();
    }
    lk.unlock();
    ApplyCommitted();
    return;
  }
  if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_recv_) >=
      election_timeout_) {
    lk.unlock();
    StartElection();
    return;
  }
}

void RaftNode::TickLoop() {
  while (true) {
    {
      std::unique_lock lk(mu_);
      cv_.wait_for(lk, kTickInterval, [this] { return !running_.load(); });
      if (!running_) {
        break;
      }
    }
    TickOnce();
  }
}

void RaftNode::Start() {
  std::scoped_lock lk(mu_);
  if (tick_thread_) {
    return;
  }
  running_ = true;
  RandomizeElectionTimeoutLocked();
  last_heartbeat_recv_ = std::chrono::steady_clock::now();
  tick_thread_ = std::make_unique<std::thread>([this] { TickLoop(); });
}

void RaftNode::Stop() {
  {
    std::scoped_lock lk(mu_);
    running_ = false;
  }
  cv_.notify_all();
  if (tick_thread_ && tick_thread_->joinable()) {
    tick_thread_->join();
    tick_thread_.reset();
  }
}

uint64_t RaftNode::Propose(RaftEntryType type, std::string payload) {
  uint64_t idx = 0;
  {
    std::scoped_lock lk(mu_);
    if (role_ != RaftRole::LEADER) {
      return 0;
    }
    idx = log_.Append(current_term_, type, std::move(payload));
    immediate_replicate_ = true;
    cv_.notify_all();
  }
  std::thread([this]() { this->SendAppendEntriesToAll(); }).detach();
  if (idx == 0) {
    return 0;
  }
  return idx;
}

AppendEntriesResponse RaftNode::HandleAppendEntries(const AppendEntriesRequest& req) {
  AppendEntriesResponse resp{};
  resp.term = current_term_;
  resp.success = false;

  {
    std::scoped_lock lk(mu_);
    if (req.term < current_term_) {
      resp.term = current_term_;
      return resp;
    }
    last_heartbeat_recv_ = std::chrono::steady_clock::now();

    if (req.term > current_term_) {
      BecomeFollower(req.term);
    } else {
      role_ = RaftRole::FOLLOWER;
    }
    leader_id_ = req.leader_id;

    if (req.prev_log_index > 0 &&
        (log_.LastIndex() < req.prev_log_index ||
         log_.TermAt(req.prev_log_index) != req.prev_log_term)) {
      resp.term = current_term_;
      return resp;
    }

    if (!req.entries.empty()) {
      log_.AppendEntries(req.entries);
    }

    const uint64_t last_idx = log_.LastIndex();
    const uint64_t before_commit = commit_index_;
    commit_index_ = std::max(commit_index_, std::min(req.leader_commit, last_idx));

    resp.success = true;
    resp.term = current_term_;
    resp.match_index = req.prev_log_index + req.entries.size();
    if (commit_index_ > before_commit) {
      cv_.notify_all();
    }
  }
  ApplyCommitted();
  return resp;
}

RequestVoteResponse RaftNode::HandleRequestVote(const RequestVoteRequest& req) {
  std::scoped_lock lk(mu_);
  RequestVoteResponse resp{current_term_, false};
  if (req.term < current_term_) {
    return resp;
  }
  last_heartbeat_recv_ = std::chrono::steady_clock::now();
  if (req.term > current_term_) {
    BecomeFollower(req.term);
  }

  const uint64_t last_idx = log_.LastIndex();
  const uint64_t last_term = log_.TermAt(last_idx);
  bool up_to_date = (req.last_log_term > last_term) ||
                    (req.last_log_term == last_term && req.last_log_index >= last_idx);
  bool grant = (!voted_for_.has_value() || *voted_for_ == req.candidate_id) && up_to_date;
  if (grant) {
    voted_for_ = req.candidate_id;
    PersistMeta();
    resp.vote_granted = true;
  }
  resp.term = current_term_;
  return resp;
}

RaftRole RaftNode::GetRole() const {
  std::scoped_lock lk(mu_);
  return role_;
}

uint64_t RaftNode::GetCurrentTerm() const {
  std::scoped_lock lk(mu_);
  return current_term_;
}

uint32_t RaftNode::GetLeaderId() const {
  std::scoped_lock lk(mu_);
  return leader_id_;
}

uint64_t RaftNode::GetCommitIndex() const {
  std::scoped_lock lk(mu_);
  return commit_index_;
}

uint64_t RaftNode::GetLastApplied() const {
  std::scoped_lock lk(mu_);
  return last_applied_;
}

const RaftLog& RaftNode::GetLog() const { return log_; }

bool RaftNode::WaitForCommit(uint64_t index, std::chrono::milliseconds timeout) {
  std::unique_lock lk(mu_);
  return cv_.wait_for(lk, timeout, [this, index] { return last_applied_ >= index; });
}

void RaftNode::LoadMeta() {
  if (meta_path_.empty()) {
    return;
  }
  std::ifstream in(meta_path_, std::ios::binary);
  if (!in) {
    return;
  }
  char buf[13];
  in.read(buf, sizeof(buf));
  if (in.gcount() != static_cast<std::streamsize>(sizeof(buf))) {
    return;
  }
  uint64_t term = 0;
  for (int i = 0; i < 8; ++i) {
    term |= static_cast<uint64_t>(static_cast<unsigned char>(buf[i])) << (8 * i);
  }
  const bool has_vote = buf[8] != 0;
  uint32_t voted_for = 0;
  for (int i = 0; i < 4; ++i) {
    voted_for |= static_cast<uint32_t>(static_cast<unsigned char>(buf[9 + i])) << (8 * i);
  }
  current_term_ = term;
  voted_for_ = has_vote ? std::optional<uint32_t>(voted_for) : std::nullopt;
}

void RaftNode::PersistMeta() const {
  if (meta_path_.empty()) {
    return;
  }
  const std::string tmp = meta_path_ + ".tmp";
  std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
  if (!out) {
    return;
  }
  char buf[13]{};
  uint64_t term = current_term_;
  for (int i = 0; i < 8; ++i) {
    buf[i] = static_cast<char>((term >> (8 * i)) & 0xffu);
  }
  buf[8] = voted_for_.has_value() ? 1 : 0;
  const uint32_t vf = voted_for_.value_or(0);
  for (int i = 0; i < 4; ++i) {
    buf[9 + i] = static_cast<char>((vf >> (8 * i)) & 0xffu);
  }
  out.write(buf, sizeof(buf));
  out.flush();
  out.close();
  if (!out) {
    return;
  }
  std::error_code ec;
  std::filesystem::rename(tmp, meta_path_, ec);
  if (ec) {
    std::filesystem::remove(meta_path_, ec);
    ec.clear();
    std::filesystem::rename(tmp, meta_path_, ec);
    if (ec) {
      std::filesystem::remove(tmp, ec);
    }
  }
}

}  // namespace txndb
