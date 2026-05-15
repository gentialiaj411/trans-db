#pragma once

#include "coordinator/catalog.h"
#include "coordinator/coordinator.h"
#include "coordinator/executor.h"
#include "coordinator/parser.h"
#include "storage/status.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using SocketType = SOCKET;
constexpr SocketType kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using SocketType = int;
constexpr SocketType kInvalidSocket = -1;
#endif

namespace txndb {

class PgWireServer {
public:
  PgWireServer(Coordinator* coordinator, Catalog* catalog, uint16_t listen_port = 5432);
  ~PgWireServer();

  Status Start();
  void Stop();

  uint16_t ListenPort() const { return bound_port_; }

private:
  void AcceptLoop();
  void HandleConnection(SocketType client_sock);
  static void ShutdownSocket(SocketType s);

  Coordinator* coordinator_;
  Catalog* catalog_;
  uint16_t requested_port_;

  SocketType listen_sock_{kInvalidSocket};
  std::atomic<bool> running_{false};
  uint16_t bound_port_{0};
  std::unique_ptr<std::thread> accept_thread_;
  std::vector<std::thread> conn_threads_;
  std::mutex conn_mu_;

  std::mutex clients_mu_;
  std::vector<SocketType> client_sockets_;
};

class PgSession {
public:
  PgSession(SocketType sock, Coordinator* coordinator, Catalog* catalog);

  void Run();

private:
  bool ReadExact(void* buf, size_t len);
  bool SendBytes(const char* buf, size_t len);

  struct RawMessage {
    char type{0};  // 0 = startup/ssl special (handled separately)
    std::string payload;
  };

  bool ReadStartupNegotiation(std::vector<char>* startup_payload_out);
  bool ReadBackendMessage(RawMessage* msg);

  void WriteStartupAck();
  void WriteAuthOk();
  void WriteParameterStatus(const std::string& key, const std::string& val);
  void WriteReadyForQuery(char txn_status);

  uint32_t PgOidFor(ColumnType ct) const;
  int16_t PgTyplenFor(ColumnType ct) const;

  void WriteRowDescription(const std::vector<std::string>& col_names,
                           const std::vector<ColumnType>& col_types);
  void WriteDataRow(const std::vector<std::string>& values);
  void WriteCommandComplete(const std::string& tag);
  void WriteErrorResponse(const std::string& severity, const std::string& code,
                          const std::string& message);
  void WriteParseComplete();
  void WriteBindComplete();
  void WriteCloseComplete();
  void WriteParameterDescription(const std::vector<uint32_t>& param_oids);
  void WriteNoData();
  void WriteEmptyQueryResponse();

  void Flush();

  bool TryParseStatement(const std::string& sql, Statement* stmt, std::string* err, size_t* err_pos);

  void EmitSuccess(const Statement& stmt, const ExecResult& res);
  char TxnIndicator() const;
  void ProcessSingle(const std::string& sql_text);
  bool DescribeSelectMeta(const std::string& query, std::vector<std::string>* col_names,
                          std::vector<ColumnType>* col_types, std::string* err_out) const;

  void HandleSimpleQuery(const std::string& sql);
  void HandleParse(const std::string& payload);
  void HandleBind(const std::string& payload);
  void HandleDescribe(const std::string& payload);
  void HandleExecute(const std::string& payload);
  void HandleSync();
  void HandleClose(const std::string& payload);

  std::vector<std::string> SplitStatements(std::string_view sql);
  std::string SubstituteParams(std::string query,
                                const std::vector<std::string>& param_texts);

  SocketType sock_{kInvalidSocket};
  Coordinator* coordinator_{nullptr};
  Catalog* catalog_{nullptr};
  Executor executor_;
  uint64_t current_txn_id_{0};
  bool txn_failed_{false};
  bool io_ok_{true};

  struct PreparedStmt {
    std::string name;
    std::string query;
    std::vector<uint32_t> param_oids;
  };
  struct Portal {
    std::string stmt_name;
    std::string query;
    std::vector<std::string> param_values;
  };

  std::unordered_map<std::string, PreparedStmt> prepared_stmts_;
  std::unordered_map<std::string, Portal> portals_;

  std::string write_buf_;
};

}  // namespace txndb
