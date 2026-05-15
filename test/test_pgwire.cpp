#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <gtest/gtest.h>

#include "shard/shard_server.h"
#include <grpcpp/grpcpp.h>

#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"
#include "pgwire/pgwire_server.h"

#include <cstring>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using SockT = SOCKET;
constexpr SockT kBadSock = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using SockT = int;
constexpr SockT kBadSock = -1;
#endif

namespace fs = std::filesystem;
using namespace txndb;

namespace pgwire_test {

void SockShutdown(SockT s) {
#ifdef _WIN32
  shutdown(s, SD_BOTH);
  closesocket(s);
#else
  shutdown(s, SHUT_RDWR);
  close(s);
#endif
}

bool ReadFull(SockT s, void* buf, size_t len) {
  char* p = static_cast<char*>(buf);
  size_t o = 0;
  while (o < len) {
#ifdef _WIN32
    const int r =
        recv(s, p + static_cast<int>(o), static_cast<int>(len - o), 0);
#else
    const ssize_t r = recv(s, p + o, len - o, 0);
#endif
    if (r <= 0) {
      return false;
    }
    o += static_cast<size_t>(r);
  }
  return true;
}

bool SendAll(SockT s, const void* buf, size_t len) {
  const char* p = static_cast<const char*>(buf);
  size_t o = 0;
  while (o < len) {
#ifdef _WIN32
    const int w =
        send(s, p + static_cast<int>(o), static_cast<int>(len - o), 0);
#else
    const ssize_t w = send(s, p + o, len - o, 0);
#endif
    if (w <= 0) {
      return false;
    }
    o += static_cast<size_t>(w);
  }
  return true;
}

uint32_t U32BE(const char* q) {
  return (static_cast<uint32_t>(static_cast<unsigned char>(q[0])) << 24) |
         (static_cast<uint32_t>(static_cast<unsigned char>(q[1])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(q[2])) << 8) |
         static_cast<uint32_t>(static_cast<unsigned char>(q[3]));
}

bool StartupClient(SockT s) {
  const std::string user = std::string("user\0postgres\0database\0db\0\0", 27);
  std::string msg;
  const uint32_t len = 4 + 4 + static_cast<uint32_t>(user.size());
  uint32_t be = htonl(len);
  msg.append(reinterpret_cast<char*>(&be), 4);
  const uint32_t ver = htonl(196608);  // 3.0
  msg.append(reinterpret_cast<const char*>(&ver), 4);
  msg.append(user);
  if (!SendAll(s, msg.data(), msg.size())) {
    return false;
  }

  bool ready = false;
  while (!ready) {
    char typ = 0;
    if (!ReadFull(s, &typ, 1)) {
      return false;
    }
    char ln[4];
    if (!ReadFull(s, ln, 4)) {
      return false;
    }
    const uint32_t mlen = U32BE(ln);
    if (mlen < 4) {
      return false;
    }
    const uint32_t plen = mlen - 4;
    std::string pay(plen, '\0');
    if (plen > 0 && !ReadFull(s, pay.data(), plen)) {
      return false;
    }
    if (typ == 'Z') {
      ready = true;
    }
  }
  return true;
}

bool ReadPgMessage(SockT s, char* typ, std::string* payload) {
  if (!ReadFull(s, typ, 1)) {
    return false;
  }
  char ln[4];
  if (!ReadFull(s, ln, 4)) {
    return false;
  }
  const uint32_t mlen = U32BE(ln);
  if (mlen < 4) {
    return false;
  }
  const uint32_t plen = mlen - 4;
  payload->assign(plen, '\0');
  if (plen > 0 && !ReadFull(s, payload->data(), plen)) {
    return false;
  }
  return true;
}

struct SimpleQueryOutcome {
  std::vector<std::string> cols;
  std::vector<std::vector<std::string>> rows;
  std::vector<std::string> command_tags;
  std::vector<std::string> errors;
};

uint16_t U16BE(const char* p) {
  return static_cast<uint16_t>((static_cast<unsigned char>(p[0]) << 8) |
                               static_cast<unsigned char>(p[1]));
}

int32_t I32BE(const char* p) {
  return static_cast<int32_t>(U32BE(p));
}

SimpleQueryOutcome ConsumeQueryResponses(SockT s) {
  SimpleQueryOutcome o;
  for (;;) {
    char typ = 0;
    std::string pay;
    if (!ReadPgMessage(s, &typ, &pay)) {
      break;
    }
    if (typ == 'T') {
      if (pay.size() < 2) {
        break;
      }
      const uint16_t nf = U16BE(pay.data());
      size_t ix = 2;
      o.cols.clear();
      for (uint16_t fi = 0; fi < nf; ++fi) {
        const size_t z = pay.find('\0', ix);
        if (z == std::string::npos || z <= ix) {
          break;
        }
        o.cols.emplace_back(&pay[ix], &pay[z]);
        if (pay.size() < z + 1 + 18) {
          break;
        }
        ix = z + 1 + 4 + 2 + 4 + 2 + 4 + 2;
      }
      continue;
    }
    if (typ == 'D') {
      if (pay.size() < 2) {
        continue;
      }
      const uint16_t nc = U16BE(pay.data());
      size_t ix = 2;
      std::vector<std::string> row;
      for (uint16_t ci = 0; ci < nc; ++ci) {
        if (ix + 4 > pay.size()) {
          break;
        }
        const int32_t ln = I32BE(pay.data() + ix);
        ix += 4;
        if (ln < 0 || ix + static_cast<size_t>(ln) > pay.size()) {
          break;
        }
        row.emplace_back(pay.substr(ix, static_cast<size_t>(ln)));
        ix += static_cast<size_t>(ln);
      }
      o.rows.push_back(std::move(row));
      continue;
    }
    if (typ == 'C') {
      if (!pay.empty() && pay.back() == '\0') {
        pay.pop_back();
      }
      o.command_tags.push_back(pay);
      continue;
    }
    if (typ == 'E') {
      auto mpos = pay.find('M');
      if (mpos != std::string::npos) {
        mpos++;
        auto end = pay.find('\0', mpos);
        if (end != std::string::npos) {
          o.errors.emplace_back(pay.substr(mpos, end - mpos));
        }
      } else {
        o.errors.emplace_back("error response");
      }
      continue;
    }
    if (typ == 'Z') {
      break;
    }
    if (typ == 'I') {
      continue;
    }
  }
  return o;
}

SimpleQueryOutcome SimpleQuery(SockT s, const std::string& sql) {
  std::string q(sql);
  q.push_back('\0');
  std::string msg;
  msg.push_back('Q');
  const uint32_t len = static_cast<uint32_t>(q.size()) + 4;
  msg.push_back(static_cast<char>((len >> 24) & 0xff));
  msg.push_back(static_cast<char>((len >> 16) & 0xff));
  msg.push_back(static_cast<char>((len >> 8) & 0xff));
  msg.push_back(static_cast<char>(len & 0xff));
  msg.append(q);
  (void)SendAll(s, msg.data(), msg.size());
  return ConsumeQueryResponses(s);
}

SockT TcpConnectlocalhost(uint16_t port) {
#ifdef _WIN32
  SockT s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
  SockT s = socket(AF_INET, SOCK_STREAM, 0);
#endif
  if (s == kBadSock) {
    return kBadSock;
  }
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
  if (connect(s, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) {
    SockShutdown(s);
    return kBadSock;
  }
  return s;
}

std::string UniqueDir() {
  const auto p =
      fs::temp_directory_path() /
      ("trans_pgwire_" + std::to_string(std::rand()));
  fs::create_directories(p);
  return p.string();
}

class EphemeralShard {
public:
  explicit EphemeralShard(uint32_t id, std::string data_dir)
      : shard_id_(id), data_dir_(std::move(data_dir)),
        impl_(std::make_unique<ShardServiceImpl>(shard_id_, data_dir_)) {}

  uint16_t Start() {
    impl_->Start();
    int pick = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &pick);
    builder.RegisterService(impl_.get());
    server_ = builder.BuildAndStart();
    if (!server_ || pick == 0) {
      return 0;
    }
    return static_cast<uint16_t>(pick);
  }

  void Stop() {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
      server_.reset();
    }
    if (impl_) {
      impl_->Stop();
    }
  }

private:
  uint32_t shard_id_;
  std::string data_dir_;
  std::unique_ptr<ShardServiceImpl> impl_;
  std::unique_ptr<grpc::Server> server_;
};

class PgWireTestEnv {
public:
  PgWireTestEnv() : base_(UniqueDir()) {
    std::srand(
        static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count()));

    std::unordered_map<uint32_t, std::string> shard_port_map;
    shards_.reserve(3);
    for (uint32_t i = 0; i < 3; ++i) {
      const std::string d = base_ + "/s" + std::to_string(i);
      shards_.push_back(std::make_unique<EphemeralShard>(i, d));
      const uint16_t p = shards_.back()->Start();
      if (p == 0) {
        throw std::runtime_error("shard listen port is 0");
      }
      shard_port_map[i] = "127.0.0.1:" + std::to_string(p);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    coord_ = std::make_unique<Coordinator>(shard_port_map, 3);

    pg_ = std::make_unique<PgWireServer>(coord_.get(), &catalog_, 0);
    if (!pg_->Start().ok()) {
      throw std::runtime_error("PgWireServer::Start failed");
    }
    pg_port_ = pg_->ListenPort();

    SockT c = TcpConnectlocalhost(pg_port_);
    if (c == kBadSock) {
      throw std::runtime_error("tcp connect pg failed");
    }
    cli_ = c;
    if (!StartupClient(cli_)) {
      throw std::runtime_error("postgres startup handshake failed");
    }
  }

  ~PgWireTestEnv() {
    if (cli_ != kBadSock) {
      SockShutdown(cli_);
      cli_ = kBadSock;
    }
    if (pg_) {
      pg_->Stop();
      pg_.reset();
    }
    coord_.reset();
    for (auto& s : shards_) {
      s->Stop();
    }
    shards_.clear();
    std::error_code ec;
    fs::remove_all(base_, ec);
  }

  uint16_t PgPort() const { return pg_port_; }
  Coordinator* Coord() { return coord_.get(); }
  Catalog* Cat() { return &catalog_; }
  SockT Cli() const { return cli_; }

private:
  std::string base_;
  std::vector<std::unique_ptr<EphemeralShard>> shards_;
  Catalog catalog_;
  std::unique_ptr<Coordinator> coord_;
  std::unique_ptr<PgWireServer> pg_;
  uint16_t pg_port_{0};
  SockT cli_{kBadSock};
};

}  // namespace pgwire_test

using namespace pgwire_test;

TEST(PgWireTest, ConnectAndStartup) {
  PgWireTestEnv env;
  EXPECT_NE(env.Cli(), kBadSock);
}

TEST(PgWireTest, CreateTableAndInsert) {
  PgWireTestEnv env;
  auto o = SimpleQuery(
      env.Cli(),
      "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");
  ASSERT_TRUE(o.errors.empty());

  auto o2 = SimpleQuery(env.Cli(), "INSERT INTO test (id, val) VALUES (1, 'hello')");
  ASSERT_TRUE(o2.errors.empty());
  ASSERT_FALSE(o2.command_tags.empty());
  EXPECT_EQ(o2.command_tags.back(), "INSERT 0 1");
}

TEST(PgWireTest, SelectAfterInsert) {
  PgWireTestEnv env;
  (void)SimpleQuery(env.Cli(),
                    "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");
  (void)SimpleQuery(env.Cli(), "INSERT INTO test (id, val) VALUES (1, 'hello')");
  auto o = SimpleQuery(env.Cli(), "SELECT * FROM test WHERE id = 1");
  ASSERT_TRUE(o.errors.empty());
  ASSERT_EQ(o.command_tags.size(), 1u);
  EXPECT_EQ(o.command_tags[0], "SELECT 1");
  ASSERT_GE(o.cols.size(), 2u);
  ASSERT_EQ(o.rows.size(), 1u);
}

TEST(PgWireTest, TransactionBlock) {
  PgWireTestEnv env;
  (void)SimpleQuery(env.Cli(),
                    "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");
  ASSERT_TRUE(SimpleQuery(env.Cli(), "BEGIN").errors.empty());

  ASSERT_TRUE(SimpleQuery(env.Cli(), "INSERT INTO test (id, val) VALUES (1, 'a')").errors.empty());
  auto o = SimpleQuery(env.Cli(), "SELECT * FROM test WHERE id = 1");
  ASSERT_TRUE(o.errors.empty());

  ASSERT_TRUE(SimpleQuery(env.Cli(), "COMMIT").errors.empty());

  auto o2 = SimpleQuery(env.Cli(), "SELECT * FROM test WHERE id = 1");
  ASSERT_TRUE(o2.errors.empty());
  ASSERT_EQ(o2.rows.size(), 1u);
}

TEST(PgWireTest, Rollback) {
  PgWireTestEnv env;
  (void)SimpleQuery(env.Cli(),
                    "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");
  ASSERT_TRUE(SimpleQuery(env.Cli(), "BEGIN").errors.empty());
  ASSERT_TRUE(SimpleQuery(env.Cli(), "INSERT INTO test (id, val) VALUES (1, 'a')").errors.empty());
  ASSERT_TRUE(SimpleQuery(env.Cli(), "ROLLBACK").errors.empty());
  auto o = SimpleQuery(env.Cli(), "SELECT * FROM test WHERE id = 1");
  ASSERT_TRUE(o.errors.empty());
  EXPECT_EQ(o.command_tags.back(), "SELECT 0");
}

TEST(PgWireTest, UpdateAndRead) {
  PgWireTestEnv env;
  (void)SimpleQuery(env.Cli(),
                    "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");
  ASSERT_TRUE(SimpleQuery(env.Cli(), "INSERT INTO test (id, val) VALUES (1, 'hello')").errors.empty());
  ASSERT_TRUE(
      SimpleQuery(env.Cli(), "UPDATE test SET val = 'world' WHERE id = 1").errors.empty());
  auto o = SimpleQuery(env.Cli(), "SELECT val FROM test WHERE id = 1");
  ASSERT_TRUE(o.errors.empty());
  ASSERT_FALSE(o.rows.empty());
  ASSERT_GE(o.rows[0].size(), 1u);
  EXPECT_EQ(o.rows[0][0], "world");
}

TEST(PgWireTest, DeleteAndRead) {
  PgWireTestEnv env;
  (void)SimpleQuery(env.Cli(),
                    "CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");
  ASSERT_TRUE(SimpleQuery(env.Cli(), "INSERT INTO test (id, val) VALUES (1, 'x')").errors.empty());
  ASSERT_TRUE(SimpleQuery(env.Cli(), "DELETE FROM test WHERE id = 1").errors.empty());
  auto o = SimpleQuery(env.Cli(), "SELECT * FROM test WHERE id = 1");
  ASSERT_TRUE(o.errors.empty());
  EXPECT_EQ(o.command_tags.back(), "SELECT 0");
}

TEST(PgWireTest, ErrorHandling) {
  PgWireTestEnv env;
  (void)SimpleQuery(env.Cli(),
                    "CREATE TABLE errt (id INT PRIMARY KEY)");
  auto bad = SimpleQuery(env.Cli(), "SELEC * FROM errt");
  ASSERT_FALSE(bad.errors.empty());

  auto good = SimpleQuery(env.Cli(), "SELECT id FROM errt WHERE id = 42");
  ASSERT_TRUE(good.errors.empty());
  EXPECT_TRUE(good.command_tags.empty() ||
              good.command_tags.back() == "SELECT 0");
}
