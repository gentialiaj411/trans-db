#pragma once

#include <string>
#include <string_view>

namespace txndb {

enum class StatusCode {
  Ok = 0,
  NotFound,
  TimedOut,
  Conflict,
  IOError,
  InvalidArgument,
  NotSupported,
};

class Status {
public:
  Status() : code_(StatusCode::Ok) {}

  static Status OK() { return Status(); }

  static Status Error(StatusCode code, std::string msg = {}) {
    Status s;
    s.code_ = code;
    s.msg_ = std::move(msg);
    return s;
  }

  static Status NotFound(std::string_view message = {}) {
    return Error(StatusCode::NotFound, std::string(message));
  }

  static Status IOError(std::string_view message) {
    return Error(StatusCode::IOError, std::string(message));
  }

  bool ok() const { return code_ == StatusCode::Ok; }

  StatusCode code() const { return code_; }

  const std::string& message() const { return msg_; }

private:
  StatusCode code_;
  std::string msg_;
};

}  // namespace txndb
