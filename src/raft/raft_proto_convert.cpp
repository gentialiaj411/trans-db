#include "raft/raft_proto_convert.h"

namespace txndb {

namespace {

uint32_t EntryTypeToWire(RaftEntryType t) { return static_cast<uint32_t>(t); }

RaftEntryType EntryTypeFromWire(uint32_t t) { return static_cast<RaftEntryType>(t); }

}  // namespace

void ToProto(const AppendEntriesRequest& in, raft::AppendEntriesRequest* out) {
  out->set_term(in.term);
  out->set_leader_id(in.leader_id);
  out->set_prev_log_index(in.prev_log_index);
  out->set_prev_log_term(in.prev_log_term);
  out->set_leader_commit(in.leader_commit);
  out->clear_entries();
  for (const auto& e : in.entries) {
    auto* msg = out->add_entries();
    msg->set_index(e.index);
    msg->set_term(e.term);
    msg->set_entry_type(EntryTypeToWire(e.type));
    msg->set_payload(e.payload);
  }
}

void FromProto(const raft::AppendEntriesRequest& in, AppendEntriesRequest* out) {
  out->term = in.term();
  out->leader_id = in.leader_id();
  out->prev_log_index = in.prev_log_index();
  out->prev_log_term = in.prev_log_term();
  out->leader_commit = in.leader_commit();
  out->entries.clear();
  out->entries.reserve(static_cast<size_t>(in.entries_size()));
  for (const auto& e : in.entries()) {
    RaftLogEntry entry;
    entry.index = e.index();
    entry.term = e.term();
    entry.type = EntryTypeFromWire(e.entry_type());
    entry.payload = e.payload();
    out->entries.push_back(std::move(entry));
  }
}

void ToProto(const AppendEntriesResponse& in, raft::AppendEntriesResponse* out) {
  out->set_term(in.term);
  out->set_success(in.success);
  out->set_match_index(in.match_index);
}

void FromProto(const raft::AppendEntriesResponse& in, AppendEntriesResponse* out) {
  out->term = in.term();
  out->success = in.success();
  out->match_index = in.match_index();
}

void ToProto(const RequestVoteRequest& in, raft::RequestVoteRequest* out) {
  out->set_term(in.term);
  out->set_candidate_id(in.candidate_id);
  out->set_last_log_index(in.last_log_index);
  out->set_last_log_term(in.last_log_term);
}

void FromProto(const raft::RequestVoteRequest& in, RequestVoteRequest* out) {
  out->term = in.term();
  out->candidate_id = in.candidate_id();
  out->last_log_index = in.last_log_index();
  out->last_log_term = in.last_log_term();
}

void ToProto(const RequestVoteResponse& in, raft::RequestVoteResponse* out) {
  out->set_term(in.term);
  out->set_vote_granted(in.vote_granted);
}

void FromProto(const raft::RequestVoteResponse& in, RequestVoteResponse* out) {
  out->term = in.term();
  out->vote_granted = in.vote_granted();
}

}  // namespace txndb
