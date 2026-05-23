#include <gtest/gtest.h>

#include "coordinator/coordinator_log.h"

#include <cstdlib>
#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using namespace txndb;

namespace {

std::string UniquePath() {
  const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
  const auto p = fs::temp_directory_path() /
                 ("trans_db_coord_log_" + std::to_string(stamp) + "_" +
                  std::to_string(std::rand()) + ".log");
  return p.string();
}

void Cleanup(const std::string& path) {
  std::error_code ec;
  fs::remove(path, ec);
}

CoordinatorLogRecord MakePreparing(uint64_t txn_id, uint64_t ts,
                                   std::vector<uint32_t> shards) {
  CoordinatorLogRecord rec;
  rec.type = CoordinatorLogRecordType::PREPARING;
  rec.txn_id = txn_id;
  rec.timestamp_us = ts;
  rec.shards = std::move(shards);
  return rec;
}

}  // namespace

TEST(CoordinatorLog, AppendReplayRoundTrip) {
  const std::string path = UniquePath();
  Cleanup(path);

  std::unique_ptr<CoordinatorLog> log;
  ASSERT_TRUE(CoordinatorLog::Open(path, &log).ok());

  CoordinatorLogRecord c1{CoordinatorLogRecordType::COMMITTING, 7, 111, {}};
  CoordinatorLogRecord c2{CoordinatorLogRecordType::COMMITTED, 7, 112, {}};
  ASSERT_TRUE(log->Append(MakePreparing(7, 110, {1, 2, 5})).ok());
  ASSERT_TRUE(log->Append(c1).ok());
  ASSERT_TRUE(log->Append(c2).ok());

  std::vector<CoordinatorLogRecord> seen;
  ASSERT_TRUE(log->Replay([&](const CoordinatorLogRecord& r) { seen.push_back(r); }).ok());
  ASSERT_EQ(seen.size(), 3u);
  EXPECT_EQ(seen[0].type, CoordinatorLogRecordType::PREPARING);
  EXPECT_EQ(seen[0].txn_id, 7u);
  EXPECT_EQ(seen[0].timestamp_us, 110u);
  ASSERT_EQ(seen[0].shards.size(), 3u);
  EXPECT_EQ(seen[0].shards[0], 1u);
  EXPECT_EQ(seen[0].shards[1], 2u);
  EXPECT_EQ(seen[0].shards[2], 5u);
  EXPECT_EQ(seen[1].type, CoordinatorLogRecordType::COMMITTING);
  EXPECT_EQ(seen[2].type, CoordinatorLogRecordType::COMMITTED);

  log.reset();
  Cleanup(path);
}

TEST(CoordinatorLog, DurabilityAcrossReopen) {
  const std::string path = UniquePath();
  Cleanup(path);
  {
    std::unique_ptr<CoordinatorLog> log;
    ASSERT_TRUE(CoordinatorLog::Open(path, &log).ok());
    ASSERT_TRUE(log->Append(MakePreparing(88, 501, {0, 2})).ok());
  }

  std::unique_ptr<CoordinatorLog> log2;
  ASSERT_TRUE(CoordinatorLog::Open(path, &log2).ok());
  std::vector<CoordinatorLogRecord> seen;
  ASSERT_TRUE(log2->Replay([&](const CoordinatorLogRecord& r) { seen.push_back(r); }).ok());
  ASSERT_EQ(seen.size(), 1u);
  EXPECT_EQ(seen[0].type, CoordinatorLogRecordType::PREPARING);
  EXPECT_EQ(seen[0].txn_id, 88u);
  EXPECT_EQ(seen[0].timestamp_us, 501u);
  ASSERT_EQ(seen[0].shards.size(), 2u);
  EXPECT_EQ(seen[0].shards[0], 0u);
  EXPECT_EQ(seen[0].shards[1], 2u);

  log2.reset();
  Cleanup(path);
}

TEST(CoordinatorLog, TornRecordIgnored) {
  const std::string path = UniquePath();
  Cleanup(path);
  {
    std::unique_ptr<CoordinatorLog> log;
    ASSERT_TRUE(CoordinatorLog::Open(path, &log).ok());
    ASSERT_TRUE(log->Append(MakePreparing(1, 1, {0, 1})).ok());
    ASSERT_TRUE(log->Append(CoordinatorLogRecord{
        CoordinatorLogRecordType::ABORTING, 1, 2, {}}).ok());
    ASSERT_TRUE(log->Append(CoordinatorLogRecord{
        CoordinatorLogRecordType::ABORTED, 1, 3, {}}).ok());
  }

  const auto sz = fs::file_size(path);
  ASSERT_GT(sz, 8u);
  std::error_code ec;
  fs::resize_file(path, sz - 7, ec);
  ASSERT_FALSE(ec);

  std::unique_ptr<CoordinatorLog> log2;
  ASSERT_TRUE(CoordinatorLog::Open(path, &log2).ok());
  int count = 0;
  ASSERT_TRUE(log2->Replay([&](const CoordinatorLogRecord&) { ++count; }).ok());
  EXPECT_EQ(count, 2);

  log2.reset();
  Cleanup(path);
}
