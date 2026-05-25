#include "pgwire/pgwire_server.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <variant>
#ifndef _WIN32
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace txndb {

namespace {

std::mutex g_wsa_mtx;
int g_wsa_users = 0;

void AppendUint32BE(std::string* b, uint32_t v) {
  b->push_back(static_cast<char>((v >> 24) & 0xFF));
  b->push_back(static_cast<char>((v >> 16) & 0xFF));
  b->push_back(static_cast<char>((v >> 8) & 0xFF));
  b->push_back(static_cast<char>(v & 0xFF));
}

void AppendUint16BE(std::string* b, uint16_t v) {
  b->push_back(static_cast<char>((v >> 8) & 0xFF));
  b->push_back(static_cast<char>(v & 0xFF));
}

uint32_t LoadUint32BE(const char* p) {
  return (static_cast<uint32_t>(static_cast<unsigned char>(p[0])) << 24) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 8) |
         static_cast<uint32_t>(static_cast<unsigned char>(p[3]));
}

uint16_t LoadUint16BE(const char* p) {
  return static_cast<uint16_t>((static_cast<unsigned char>(p[0]) << 8) |
                                 static_cast<unsigned char>(p[1]));
}

int32_t LoadInt32BE(const char* p) {
  return static_cast<int32_t>(LoadUint32BE(p));
}

std::string_view TrimSv(std::string_view s) {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
    s.remove_prefix(1);
  }
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
    s.remove_suffix(1);
  }
  return s;
}

bool LooksLikeNumber(std::string_view s) {
  if (s.empty()) {
    return false;
  }
  size_t i = 0;
  if (s[0] == '+' || s[0] == '-') {
    ++i;
  }
  bool saw_digit = false;
  bool saw_dot = false;
  for (; i < s.size(); ++i) {
    const unsigned char c = static_cast<unsigned char>(s[i]);
    if (std::isdigit(c)) {
      saw_digit = true;
      continue;
    }
    if (c == '.' && !saw_dot) {
      saw_dot = true;
      continue;
    }
    return false;
  }
  return saw_digit;
}

std::string SqlQuoteLiteral(std::string_view text) {
  std::string o;
  o.push_back('\'');
  for (char c : text) {
    if (c == '\'') {
      o.append("''");
    } else {
      o.push_back(c);
    }
  }
  o.push_back('\'');
  return o;
}

template <typename Fn>
bool ParseCStringBlob(std::string_view* blob, std::string_view* field, Fn consume) {
  const size_t n = blob->find('\0');
  if (n == std::string_view::npos) {
    return false;
  }
  *field = blob->substr(0, n);
  blob->remove_prefix(n + 1);
  (void)consume;
  return true;
}

bool ParseCString(std::string_view* blob, std::string* out) {
  std::string_view sv;
  if (!ParseCStringBlob(blob, &sv, [] {})) {
    return false;
  }
  out->assign(sv.begin(), sv.end());
  return true;
}

const ColumnDef* LookupColumn(const TableDef& table, std::string_view name) {
  for (const auto& c : table.columns) {
    if (c.name == name) {
      return &c;
    }
  }
  return nullptr;
}

std::string UnqualifyName(std::string_view name) {
  const size_t dot = name.rfind('.');
  if (dot == std::string_view::npos) {
    return std::string(name);
  }
  return std::string(name.substr(dot + 1));
}

}  // namespace

// --- PgWireServer ------------------------------------------------------------

PgWireServer::PgWireServer(Coordinator* coordinator, Catalog* catalog, uint16_t listen_port)
    : coordinator_(coordinator),
      catalog_(catalog),
      requested_port_(listen_port) {}

PgWireServer::~PgWireServer() { Stop(); }

void PgWireServer::ShutdownSocket(SocketType s) {
  if (s == kInvalidSocket) {
    return;
  }
#ifdef _WIN32
  shutdown(s, SD_BOTH);
  closesocket(s);
#else
  shutdown(s, SHUT_RDWR);
  close(s);
#endif
}

Status PgWireServer::Start() {
  if (running_.exchange(true)) {
    return Status::Error(StatusCode::InvalidArgument, "already started");
  }
#ifdef _WIN32
  {
    std::scoped_lock lk(g_wsa_mtx);
    if (g_wsa_users++ == 0) {
      WSADATA wsaData{};
      if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        --g_wsa_users;
        running_.store(false);
        return Status::IOError("WSAStartup failed");
      }
    }
  }
#endif

#ifdef _WIN32
  listen_sock_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listen_sock_ == INVALID_SOCKET) {
#ifdef _WIN32
    running_.store(false);
    std::scoped_lock lk(g_wsa_mtx);
    if (--g_wsa_users == 0) {
      WSACleanup();
    }
#endif
    return Status::IOError("socket");
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(requested_port_);

  BOOL yes = TRUE;
  setsockopt(listen_sock_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&yes), sizeof(yes));

  if (bind(listen_sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
      listen(listen_sock_, SOMAXCONN) != 0) {
    ShutdownSocket(listen_sock_);
    listen_sock_ = kInvalidSocket;
    running_.store(false);
#ifdef _WIN32
    std::scoped_lock lk(g_wsa_mtx);
    if (--g_wsa_users == 0) {
      WSACleanup();
    }
#endif
    return Status::IOError("bind/listen");
  }

  socklen_t alen = sizeof(addr);
  if (getsockname(listen_sock_, reinterpret_cast<sockaddr*>(&addr), &alen) == 0) {
    bound_port_ = ntohs(addr.sin_port);
  }
#else
  listen_sock_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_sock_ < 0) {
    running_.store(false);
    return Status::IOError("socket");
  }
  int yes = 1;
  setsockopt(listen_sock_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(requested_port_);

  if (bind(listen_sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
      listen(listen_sock_, SOMAXCONN) != 0) {
    ShutdownSocket(listen_sock_);
    listen_sock_ = kInvalidSocket;
    running_.store(false);
    return Status::IOError("bind/listen");
  }

  socklen_t alen = sizeof(addr);
  if (getsockname(listen_sock_, reinterpret_cast<sockaddr*>(&addr), &alen) == 0) {
    bound_port_ = ntohs(addr.sin_port);
  }
#endif

  accept_thread_ = std::make_unique<std::thread>([this] { AcceptLoop(); });
  return Status::OK();
}

void PgWireServer::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  ShutdownSocket(listen_sock_);
  listen_sock_ = kInvalidSocket;

  std::vector<SocketType> snap;
  {
    std::scoped_lock lk(clients_mu_);
    snap = std::move(client_sockets_);
  }
  for (SocketType s : snap) {
    ShutdownSocket(s);
  }

  if (accept_thread_ && accept_thread_->joinable()) {
    accept_thread_->join();
  }
  accept_thread_.reset();

  {
    std::scoped_lock lk(conn_mu_);
    for (auto& t : conn_threads_) {
      if (t.joinable()) {
        t.join();
      }
    }
    conn_threads_.clear();
  }

#ifdef _WIN32
  std::scoped_lock lk(g_wsa_mtx);
  if (--g_wsa_users == 0) {
    WSACleanup();
  }
#endif
}

void PgWireServer::AcceptLoop() {
  while (running_.load()) {
#ifdef _WIN32
    SOCKET c = ::accept(listen_sock_, nullptr, nullptr);
    if (c == INVALID_SOCKET || !running_.load()) {
      break;
    }
#else
    int c = ::accept(listen_sock_, nullptr, nullptr);
    if (c < 0 || !running_.load()) {
      break;
    }
#endif

    {
      std::scoped_lock lk(clients_mu_);
      client_sockets_.push_back(c);
    }

    std::thread worker([this, c] {
      PgSession sess(c, coordinator_, catalog_);
      sess.Run();
      {
        std::scoped_lock l2(clients_mu_);
        auto& v = client_sockets_;
        v.erase(std::remove(v.begin(), v.end(), c), v.end());
      }
      ShutdownSocket(c);
    });
    std::scoped_lock lk(conn_mu_);
    conn_threads_.emplace_back(std::move(worker));
  }
}

// --- PgSession --------------------------------------------------------------

PgSession::PgSession(SocketType sock, Coordinator* coordinator, Catalog* catalog)
    : sock_(sock), coordinator_(coordinator), catalog_(catalog), executor_(coordinator_, catalog_) {}

bool PgSession::ReadExact(void* buf, size_t len) {
  char* p = static_cast<char*>(buf);
  size_t off = 0;
  while (off < len) {
#ifdef _WIN32
    const int r =
        recv(sock_, p + static_cast<int>(off), static_cast<int>(len - off), 0);
    if (r <= 0) {
      io_ok_ = false;
      return false;
    }
    off += static_cast<size_t>(r);
#else
    const ssize_t r = recv(sock_, p + off, len - off, 0);
    if (r <= 0) {
      io_ok_ = false;
      return false;
    }
    off += static_cast<size_t>(r);
#endif
  }
  return true;
}

bool PgSession::SendBytes(const char* buf, size_t len) {
  size_t off = 0;
  while (off < len) {
#ifdef _WIN32
    const int s =
        send(sock_, buf + static_cast<int>(off), static_cast<int>(len - off), 0);
    if (s <= 0) {
      io_ok_ = false;
      return false;
    }
    off += static_cast<size_t>(s);
#else
    const ssize_t s = send(sock_, buf + off, len - off, 0);
    if (s <= 0) {
      io_ok_ = false;
      return false;
    }
    off += static_cast<size_t>(s);
#endif
  }
  return true;
}

void PgSession::Flush() {
  if (write_buf_.empty()) {
    return;
  }
  (void)SendBytes(write_buf_.data(), write_buf_.size());
  write_buf_.clear();
}

bool PgSession::ReadStartupNegotiation(std::vector<char>* out_body) {
  for (;;) {
    char lenb[4];
    if (!ReadExact(lenb, 4)) {
      return false;
    }
    const uint32_t total = LoadUint32BE(lenb);
    if (total < 8) {
      io_ok_ = false;
      return false;
    }
    if (total == 8) {
      char codeb[4];
      if (!ReadExact(codeb, 4)) {
        return false;
      }
      const uint32_t code = LoadUint32BE(codeb);
      if (code == 80877103UL) {
        if (!SendBytes("N", 1)) {
          return false;
        }
        continue;
      }
    }
    out_body->resize(total);
    std::memcpy(out_body->data(), lenb, 4);
    if (!ReadExact(out_body->data() + 4, total - 4)) {
      return false;
    }
    break;
  }
  return true;
}

bool PgSession::ReadBackendMessage(RawMessage* msg) {
  char type;
  char lenb[4];
  if (!ReadExact(&type, 1)) {
    return false;
  }
  if (!ReadExact(lenb, 4)) {
    return false;
  }
  const uint32_t len = LoadUint32BE(lenb);
  if (len < 4) {
    io_ok_ = false;
    return false;
  }
  const uint32_t plen = len - 4;
  msg->type = type;
  msg->payload.assign(plen, '\0');
  if (plen > 0 && !ReadExact(msg->payload.data(), plen)) {
    return false;
  }
  return true;
}

void PgSession::WriteAuthOk() {
  std::string p;
  AppendUint32BE(&p, 0);
  write_buf_.push_back('R');
  AppendUint32BE(&write_buf_, 4 + static_cast<uint32_t>(p.size()));
  write_buf_.append(p);
}

void PgSession::WriteParameterStatus(const std::string& key, const std::string& val) {
  std::string p;
  p.append(key);
  p.push_back('\0');
  p.append(val);
  p.push_back('\0');
  write_buf_.push_back('S');
  AppendUint32BE(&write_buf_, 4 + static_cast<uint32_t>(p.size()));
  write_buf_.append(p);
}

void PgSession::WriteReadyForQuery(char txn_status) {
  write_buf_.push_back('Z');
  AppendUint32BE(&write_buf_, 5);
  write_buf_.push_back(txn_status);
}

void PgSession::WriteStartupAck() {
  WriteAuthOk();
  WriteParameterStatus("server_version", "15.0");
  WriteParameterStatus("server_encoding", "UTF8");
  WriteParameterStatus("client_encoding", "UTF8");
  WriteParameterStatus("DateStyle", "ISO, MDY");
  WriteReadyForQuery(TxnIndicator());
  Flush();
}

uint32_t PgSession::PgOidFor(ColumnType ct) const {
  switch (ct) {
    case ColumnType::INT:
      return 23;
    case ColumnType::BIGINT:
      return 20;
    case ColumnType::FLOAT:
      return 701;
    case ColumnType::VARCHAR:
      return 25;
  }
  return 25;
}

int16_t PgSession::PgTyplenFor(ColumnType ct) const {
  switch (ct) {
    case ColumnType::INT:
      return 4;
    case ColumnType::BIGINT:
      return 8;
    case ColumnType::FLOAT:
      return 8;
    case ColumnType::VARCHAR:
      return -1;
  }
  return -1;
}

void PgSession::WriteRowDescription(const std::vector<std::string>& col_names,
                                    const std::vector<ColumnType>& col_types) {
  std::string payload;
  AppendUint16BE(&payload, static_cast<uint16_t>(col_names.size()));
  for (size_t i = 0; i < col_names.size(); ++i) {
    payload.append(col_names[i]);
    payload.push_back('\0');
    AppendUint32BE(&payload, 0);                                  // table oid
    AppendUint16BE(&payload, 0);                                // attribute number
    AppendUint32BE(&payload, PgOidFor(col_types[i]));
    AppendUint16BE(&payload, static_cast<uint16_t>(PgTyplenFor(col_types[i])));
    AppendUint32BE(&payload, -1);  // typmod signed as uint32 blob
    AppendUint16BE(&payload, 0);   // text format
  }
  write_buf_.push_back('T');
  AppendUint32BE(&write_buf_, 4 + static_cast<uint32_t>(payload.size()));
  write_buf_.append(payload);
}

void PgSession::WriteDataRow(const std::vector<std::string>& values) {
  std::string payload;
  AppendUint16BE(&payload, static_cast<uint16_t>(values.size()));
  for (const std::string& v : values) {
    AppendUint32BE(&payload, static_cast<uint32_t>(v.size()));
    payload.append(v);
  }
  write_buf_.push_back('D');
  AppendUint32BE(&write_buf_, 4 + static_cast<uint32_t>(payload.size()));
  write_buf_.append(payload);
}

void PgSession::WriteCommandComplete(const std::string& tag) {
  std::string p = tag;
  p.push_back('\0');
  write_buf_.push_back('C');
  AppendUint32BE(&write_buf_, 4 + static_cast<uint32_t>(p.size()));
  write_buf_.append(p);
}

void PgSession::WriteErrorResponse(const std::string& severity, const std::string& code,
                                   const std::string& message) {
  std::string p;
  p.push_back('S');
  p.append(severity);
  p.push_back('\0');
  p.push_back('C');
  p.append(code);
  p.push_back('\0');
  p.push_back('M');
  p.append(message);
  p.push_back('\0');
  p.push_back('\0');
  write_buf_.push_back('E');
  AppendUint32BE(&write_buf_, 4 + static_cast<uint32_t>(p.size()));
  write_buf_.append(p);
  Flush();
}

void PgSession::WriteParseComplete() {
  write_buf_.push_back('1');
  AppendUint32BE(&write_buf_, 4);
}

void PgSession::WriteBindComplete() {
  write_buf_.push_back('2');
  AppendUint32BE(&write_buf_, 4);
}

void PgSession::WriteCloseComplete() {
  write_buf_.push_back('3');
  AppendUint32BE(&write_buf_, 4);
}

void PgSession::WriteParameterDescription(const std::vector<uint32_t>& param_oids) {
  std::string payload;
  AppendUint16BE(&payload, static_cast<uint16_t>(param_oids.size()));
  for (uint32_t oid : param_oids) {
    AppendUint32BE(&payload, oid);
  }
  write_buf_.push_back('t');
  AppendUint32BE(&write_buf_, 4 + static_cast<uint32_t>(payload.size()));
  write_buf_.append(payload);
}

void PgSession::WriteNoData() {
  write_buf_.push_back('n');
  AppendUint32BE(&write_buf_, 4);
}

void PgSession::WriteEmptyQueryResponse() {
  write_buf_.push_back('I');
  AppendUint32BE(&write_buf_, 4);
}

bool PgSession::TryParseStatement(const std::string& sql, Statement* stmt, std::string* err,
                                  size_t* err_pos) {
  auto pv = Parser::Parse(sql);
  if (ParseError* pe = std::get_if<ParseError>(&pv)) {
    *err = pe->message;
    *err_pos = pe->position;
    return false;
  }
  *stmt = std::get<Statement>(std::move(pv));
  return true;
}

std::vector<std::string> PgSession::SplitStatements(std::string_view sql) {
  std::vector<std::string> out;
  bool in_quote = false;
  std::string cur;
  for (size_t i = 0; i < sql.size(); ++i) {
    const char c = sql[i];
    if (c == '\'') {
      in_quote = !in_quote;
      cur.push_back(c);
      continue;
    }
    if (!in_quote && c == ';') {
      const std::string_view part = TrimSv(cur);
      if (!part.empty()) {
        out.emplace_back(part.begin(), part.end());
      }
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  const std::string_view part = TrimSv(cur);
  if (!part.empty()) {
    out.emplace_back(part.begin(), part.end());
  }
  return out;
}

std::string PgSession::SubstituteParams(std::string query,
                                        const std::vector<std::string>& param_texts) {
  const size_t n = param_texts.size();
  for (size_t i = n; i >= 1; --i) {
    const std::string pat = '$' + std::to_string(i);
    std::string rep;
    const std::string& raw = param_texts[i - 1];
    if (LooksLikeNumber(raw)) {
      rep = raw;
    } else {
      rep = SqlQuoteLiteral(raw);
    }

    size_t pos = 0;
    while ((pos = query.find(pat, pos)) != std::string::npos) {
      const bool ok_left =
          pos == 0 || !std::isalnum(static_cast<unsigned char>(query[pos - 1]));
      const bool ok_right =
          pos + pat.size() >= query.size() ||
          !std::isdigit(static_cast<unsigned char>(query[pos + pat.size()]));
      if (!(ok_left && ok_right)) {
        pos += pat.size();
        continue;
      }
      query.replace(pos, pat.size(), rep);
      pos += rep.size();
    }
  }
  return query;
}

void PgSession::EmitSuccess(const Statement& stmt, const ExecResult& res) {
  if (!res.ok) {
    return;
  }

  const auto infer_select_types = [&](const SelectStmt& sel, const std::vector<std::string>& out_cols,
                                      std::vector<ColumnType>* out_types) -> bool {
    const TableDef* left = catalog_->GetTable(sel.table_name);
    if (!left) {
      WriteErrorResponse("ERROR", "XX000", "unknown table");
      return false;
    }
    const TableDef* right = nullptr;
    if (sel.join.has_value()) {
      right = catalog_->GetTable(sel.join->table_name);
      if (!right) {
        WriteErrorResponse("ERROR", "XX000", "unknown table");
        return false;
      }
    }

    const auto resolve_col = [&](std::string_view raw) -> std::optional<ColumnType> {
      const std::string unq = UnqualifyName(raw);
      if (const ColumnDef* c = LookupColumn(*left, raw)) {
        return c->type;
      }
      if (const ColumnDef* c = LookupColumn(*left, unq)) {
        return c->type;
      }
      if (right != nullptr) {
        if (const ColumnDef* c = LookupColumn(*right, raw)) {
          return c->type;
        }
        if (const ColumnDef* c = LookupColumn(*right, unq)) {
          return c->type;
        }
      }
      return std::nullopt;
    };

    out_types->clear();
    out_types->reserve(out_cols.size());

    if (!sel.aggregates.empty()) {
      size_t agg_ix = 0;
      for (size_t i = 0; i < out_cols.size(); ++i) {
        if (i < sel.columns.size()) {
          std::optional<ColumnType> ty = resolve_col(sel.columns[i]);
          out_types->push_back(ty.value_or(ColumnType::VARCHAR));
          continue;
        }
        const auto& agg = sel.aggregates[agg_ix++];
        if (agg.func == SelectStmt::Aggregate::Func::COUNT) {
          out_types->push_back(ColumnType::BIGINT);
          continue;
        }
        std::optional<ColumnType> ty = agg.star ? std::optional<ColumnType>(ColumnType::BIGINT)
                                                : resolve_col(agg.column);
        out_types->push_back(ty.value_or(ColumnType::VARCHAR));
      }
      return true;
    }

    for (const std::string& out_col : out_cols) {
      std::optional<ColumnType> ty = resolve_col(out_col);
      out_types->push_back(ty.value_or(ColumnType::VARCHAR));
    }
    return true;
  };

  const auto emit_select_shapes = [&](const SelectStmt& sel, const ExecResult& r) -> bool {
    std::vector<ColumnType> types;
    if (!infer_select_types(sel, r.columns, &types)) {
      return false;
    }
    WriteRowDescription(r.columns, types);
    for (const auto& rr : r.rows) {
      WriteDataRow(rr.values);
    }
    WriteCommandComplete(r.command_tag);
    return true;
  };

  if (const auto* sel = std::get_if<SelectStmt>(&stmt)) {
    (void)emit_select_shapes(*sel, res);
    return;
  }

  WriteCommandComplete(res.command_tag);
}

char PgSession::TxnIndicator() const {
  if (txn_failed_) {
    return 'E';
  }
  if (current_txn_id_ != 0) {
    return 'T';
  }
  return 'I';
}

void PgSession::ProcessSingle(const std::string& sql_text) {
  Statement stmt;
  std::string perr;
  size_t pos_err = 0;
  if (!TryParseStatement(sql_text, &stmt, &perr, &pos_err)) {
    WriteErrorResponse("ERROR", "42601", perr);
    if (current_txn_id_ != 0) {
      txn_failed_ = true;
    }
    return;
  }

  const bool roll = std::holds_alternative<RollbackStmt>(stmt);
  if (txn_failed_ && !roll) {
    WriteErrorResponse(
        "ERROR", "25P02",
        "current transaction is aborted, commands ignored until end of transaction block");
    return;
  }

  Executor::ExecOutput out = executor_.Execute(stmt, current_txn_id_);
  current_txn_id_ = out.txn_id;

  if (!out.result.ok) {
    WriteErrorResponse("ERROR", "XX000", out.result.error.empty() ? "error" : out.result.error);
    if (current_txn_id_ != 0) {
      txn_failed_ = true;
    }
    return;
  }

  if (roll || std::holds_alternative<CommitStmt>(stmt)) {
    txn_failed_ = false;
  }

  EmitSuccess(stmt, out.result);
}

void PgSession::HandleSimpleQuery(const std::string& payload) {
  std::string sql(payload);
  while (!sql.empty() && sql.back() == '\0') {
    sql.pop_back();
  }
  const auto parts = SplitStatements(sql);
  if (parts.empty()) {
    WriteEmptyQueryResponse();
  } else {
    for (const std::string& chunk : parts) {
      ProcessSingle(chunk);
      if (!io_ok_) {
        return;
      }
    }
  }
  WriteReadyForQuery(TxnIndicator());
  Flush();
}

void PgSession::HandleParse(const std::string& payload) {
  std::string_view b(payload);
  std::string stmt_name;
  std::string query;
  if (!ParseCString(&b, &stmt_name) || !ParseCString(&b, &query)) {
    WriteErrorResponse("ERROR", "08P01", "invalid Parse message");
    return;
  }
  if (b.size() < 2) {
    WriteErrorResponse("ERROR", "08P01", "truncated Parse message");
    return;
  }

  PreparedStmt ps;
  ps.name = stmt_name;
  ps.query = query;
  const uint16_t ntypes = LoadUint16BE(b.data());
  b.remove_prefix(2);
  for (uint16_t i = 0; i < ntypes; ++i) {
    if (b.size() < 4) {
      WriteErrorResponse("ERROR", "08P01", "truncated Parse param types");
      return;
    }
    ps.param_oids.push_back(LoadUint32BE(b.data()));
    b.remove_prefix(4);
  }
  prepared_stmts_[stmt_name] = std::move(ps);

  WriteParseComplete();
}

void PgSession::HandleBind(const std::string& payload) {
  std::string_view b(payload);
  std::string portal_name;
  std::string stmt_name;
  if (!ParseCString(&b, &portal_name) || !ParseCString(&b, &stmt_name)) {
    WriteErrorResponse("ERROR", "08P01", "invalid Bind message");
    return;
  }

  auto read_i16_sv = [&](uint16_t* o) -> bool {
    if (b.size() < 2) {
      return false;
    }
    *o = LoadUint16BE(b.data());
    b.remove_prefix(2);
    return true;
  };

  auto read_i32_sv = [&](int32_t* o) -> bool {
    if (b.size() < 4) {
      return false;
    }
    *o = LoadInt32BE(b.data());
    b.remove_prefix(4);
    return true;
  };

  uint16_t num_formats = 0;
  if (!read_i16_sv(&num_formats)) {
    WriteErrorResponse("ERROR", "08P01", "invalid Bind message");
    return;
  }

  for (uint16_t i = 0; i < num_formats; ++i) {
    if (b.size() < 2) {
      WriteErrorResponse("ERROR", "08P01", "truncated Bind formats");
      return;
    }
    b.remove_prefix(2);
  }

  uint16_t num_params = 0;
  if (!read_i16_sv(&num_params)) {
    WriteErrorResponse("ERROR", "08P01", "invalid Bind param count");
    return;
  }

  std::vector<std::string> params;
  params.reserve(num_params);
  for (uint16_t i = 0; i < num_params; ++i) {
    int32_t plen = 0;
    if (!read_i32_sv(&plen)) {
      WriteErrorResponse("ERROR", "08P01", "truncated Bind params");
      return;
    }
    if (plen < 0) {
      params.emplace_back("");
      continue;
    }
    if (static_cast<uint32_t>(plen) > b.size()) {
      WriteErrorResponse("ERROR", "08P01", "bad Bind param length");
      return;
    }
    params.emplace_back(b.substr(0, static_cast<size_t>(plen)));
    b.remove_prefix(static_cast<size_t>(plen));
  }

  uint16_t rformats = 0;
  if (!read_i16_sv(&rformats)) {
    WriteErrorResponse("ERROR", "08P01", "invalid Bind result formats");
    return;
  }

  auto psi = prepared_stmts_.find(stmt_name);
  if (psi == prepared_stmts_.end()) {
    psi = prepared_stmts_.find("");
  }
  if (psi == prepared_stmts_.end()) {
    WriteErrorResponse("ERROR", "26000", "unknown prepared statement");
    return;
  }

  Portal port;
  port.stmt_name = stmt_name;
  port.query = SubstituteParams(psi->second.query, params);
  port.param_values = std::move(params);
  portals_[portal_name] = std::move(port);

  (void)rformats;
  WriteBindComplete();
}

bool PgSession::DescribeSelectMeta(const std::string& query,
                                   std::vector<std::string>* col_names,
                                   std::vector<ColumnType>* col_types, std::string* err_out) const {
  Statement stmt;
  std::string perr;
  size_t pepos = 0;
  if (!const_cast<PgSession*>(this)->TryParseStatement(query, &stmt, &perr, &pepos)) {
    *err_out = perr;
    return false;
  }
  const auto* sel = std::get_if<SelectStmt>(&stmt);
  if (!sel) {
    return false;
  }
  Executor::ExecOutput out = const_cast<PgSession*>(this)->executor_.Execute(stmt, current_txn_id_);
  if (!out.result.ok) {
    *err_out = out.result.error.empty() ? "execute failed" : out.result.error;
    return false;
  }
  *col_names = out.result.columns;

  Statement parsed = std::move(stmt);
  const auto* psel = std::get_if<SelectStmt>(&parsed);
  if (psel == nullptr) {
    return false;
  }
  col_types->clear();
  col_types->reserve(col_names->size());

  const TableDef* left = catalog_->GetTable(psel->table_name);
  if (!left) {
    *err_out = "unknown table";
    return false;
  }
  const TableDef* right = nullptr;
  if (psel->join.has_value()) {
    right = catalog_->GetTable(psel->join->table_name);
    if (!right) {
      *err_out = "unknown table";
      return false;
    }
  }
  auto resolve_col = [&](std::string_view raw) -> std::optional<ColumnType> {
    const std::string unq = UnqualifyName(raw);
    if (const ColumnDef* c = LookupColumn(*left, raw)) return c->type;
    if (const ColumnDef* c = LookupColumn(*left, unq)) return c->type;
    if (right != nullptr) {
      if (const ColumnDef* c = LookupColumn(*right, raw)) return c->type;
      if (const ColumnDef* c = LookupColumn(*right, unq)) return c->type;
    }
    return std::nullopt;
  };
  if (!psel->aggregates.empty()) {
    size_t agg_ix = 0;
    for (size_t i = 0; i < col_names->size(); ++i) {
      if (i < psel->columns.size()) {
        col_types->push_back(resolve_col(psel->columns[i]).value_or(ColumnType::VARCHAR));
      } else {
        const auto& agg = psel->aggregates[agg_ix++];
        if (agg.func == SelectStmt::Aggregate::Func::COUNT) {
          col_types->push_back(ColumnType::BIGINT);
        } else {
          col_types->push_back(resolve_col(agg.column).value_or(ColumnType::VARCHAR));
        }
      }
    }
  } else {
    for (const std::string& n : *col_names) {
      col_types->push_back(resolve_col(n).value_or(ColumnType::VARCHAR));
    }
  }
  return true;
}

void PgSession::HandleDescribe(const std::string& payload) {
  if (payload.empty()) {
    WriteErrorResponse("ERROR", "08P01", "Describe message too short");
    return;
  }
  const char kind = payload[0];
  std::string_view b(payload.substr(1));
  std::string name;
  if (!ParseCString(&b, &name)) {
    WriteErrorResponse("ERROR", "08P01", "invalid Describe name");
    return;
  }

  if (kind == 'S') {
    auto it = prepared_stmts_.find(name);
    if (it == prepared_stmts_.end()) {
      it = prepared_stmts_.find("");
    }
    if (it == prepared_stmts_.end()) {
      WriteErrorResponse("ERROR", "26000", "unknown prepared statement");
      return;
    }
    WriteParameterDescription(it->second.param_oids);

    std::vector<std::string> cnames;
    std::vector<ColumnType> ctypes;
    std::string terr;
    if (DescribeSelectMeta(it->second.query, &cnames, &ctypes, &terr)) {
      WriteRowDescription(cnames, ctypes);
    } else {
      WriteNoData();
    }
    return;
  }

  if (kind == 'P') {
    WriteNoData();
    return;
  }

  WriteErrorResponse("ERROR", "08P01", "unknown Describe kind");
}

void PgSession::HandleExecute(const std::string& payload) {
  std::string_view b(payload);
  std::string portal_name;
  if (!ParseCString(&b, &portal_name)) {
    WriteErrorResponse("ERROR", "08P01", "invalid Execute portal");
    return;
  }

  if (!b.empty()) {
    if (b.size() < 4) {
      WriteErrorResponse("ERROR", "08P01", "truncated Execute message");
      return;
    }
    b.remove_prefix(4);
  }

  auto pit = portals_.find(portal_name);
  if (pit == portals_.end()) {
    pit = portals_.find("");
  }
  if (pit == portals_.end()) {
    WriteErrorResponse("ERROR", "34000", "unknown portal");
    return;
  }

  ProcessSingle(pit->second.query);
}

void PgSession::HandleSync() {
  WriteReadyForQuery(TxnIndicator());
  Flush();
}

void PgSession::HandleClose(const std::string& payload) {
  if (payload.empty()) {
    WriteErrorResponse("ERROR", "08P01", "Close message too short");
    return;
  }
  const char kind = payload[0];
  std::string_view b(payload.substr(1));
  std::string name;
  if (!ParseCString(&b, &name)) {
    WriteErrorResponse("ERROR", "08P01", "invalid Close name");
    return;
  }

  if (kind == 'S') {
    prepared_stmts_.erase(name);
  } else if (kind == 'P') {
    portals_.erase(name);
  } else {
    WriteErrorResponse("ERROR", "08P01", "unknown Close kind");
    return;
  }
  WriteCloseComplete();
}

void PgSession::Run() {
  std::vector<char> startup_body;
  if (!ReadStartupNegotiation(&startup_body)) {
    return;
  }
  (void)startup_body;

  WriteStartupAck();

  while (io_ok_) {
    RawMessage m;
    if (!ReadBackendMessage(&m)) {
      break;
    }
    switch (m.type) {
      case 'X':
        return;
      case 'Q':
        HandleSimpleQuery(m.payload);
        break;
      case 'P':
        HandleParse(m.payload);
        break;
      case 'B':
        HandleBind(m.payload);
        break;
      case 'E':
        HandleExecute(m.payload);
        break;
      case 'D':
        HandleDescribe(m.payload);
        break;
      case 'S':
        HandleSync();
        break;
      case 'H':
        Flush();
        break;
      case 'C':
        HandleClose(m.payload);
        break;
      default:
        break;
    }
  }
}

}  // namespace txndb
