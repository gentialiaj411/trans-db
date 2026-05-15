#include "raft/raft_log.h"

#include <algorithm>

namespace txndb {

uint64_t RaftLog::Append(uint64_t term, RaftEntryType type, std::string payload) {
  std::scoped_lock lk(mu_);
  const uint64_t idx = entries_.empty() ? 1u : (entries_.back().index + 1);
  RaftLogEntry e;
  e.index = idx;
  e.term = term;
  e.type = type;
  e.payload = std::move(payload);
  entries_.push_back(std::move(e));
  return idx;
}

std::optional<RaftLogEntry> RaftLog::Get(uint64_t index) const {
  std::scoped_lock lk(mu_);
  if (index == 0 || entries_.empty() || index > entries_.back().index) {
    return std::nullopt;
  }
  if (index < entries_.front().index) {
    return std::nullopt;
  }
  return entries_[index - entries_.front().index];
}

std::vector<RaftLogEntry> RaftLog::GetRange(uint64_t start_index, uint64_t end_index) const {
  std::scoped_lock lk(mu_);
  std::vector<RaftLogEntry> out;
  if (entries_.empty() || start_index == 0 || end_index < start_index) {
    return out;
  }
  const uint64_t first = entries_.front().index;
  const uint64_t last = entries_.back().index;
  const uint64_t s = std::max(start_index, first);
  const uint64_t e = std::min(end_index, last);
  for (uint64_t i = s; i <= e; ++i) {
    out.push_back(entries_[i - first]);
  }
  return out;
}

uint64_t RaftLog::LastIndex() const {
  std::scoped_lock lk(mu_);
  return entries_.empty() ? 0u : entries_.back().index;
}

uint64_t RaftLog::TermAt(uint64_t index) const {
  std::scoped_lock lk(mu_);
  if (index == 0 || entries_.empty()) {
    return 0u;
  }
  const uint64_t first = entries_.front().index;
  if (index < first || index > entries_.back().index) {
    return 0u;
  }
  return entries_[index - first].term;
}

void RaftLog::TruncateFrom(uint64_t index) {
  std::scoped_lock lk(mu_);
  if (entries_.empty() || index == 0) {
    return;
  }
  const uint64_t first = entries_.front().index;
  if (index > entries_.back().index) {
    return;
  }
  const size_t erase_from = static_cast<size_t>(index - first);
  if (erase_from >= entries_.size()) {
    return;
  }
  entries_.erase(entries_.begin() + static_cast<std::ptrdiff_t>(erase_from), entries_.end());
}

void RaftLog::AppendEntries(const std::vector<RaftLogEntry>& entries) {
  std::scoped_lock lk(mu_);
  for (const auto& in : entries) {
    if (!entries_.empty()) {
      const uint64_t cur_last = entries_.back().index;
      if (in.index <= cur_last) {
        const size_t off = static_cast<size_t>(in.index - entries_.front().index);
        if (entries_[off].term != in.term) {
          entries_.erase(entries_.begin() + static_cast<std::ptrdiff_t>(off), entries_.end());
        } else {
          continue;
        }
      }
    }
    const uint64_t expect = entries_.empty() ? 1u : (entries_.back().index + 1);
    if (in.index != expect) {
      continue;
    }
    entries_.push_back(in);
  }
}

}  // namespace txndb
