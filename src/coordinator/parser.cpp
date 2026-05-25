#include "coordinator/parser.h"

#include <cctype>
#include <charconv>
#include <string>

namespace txndb {

namespace {

bool IsIdentStart(char c) {
  return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
}

bool IsIdentCont(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

void SkipWsAndComments(std::string_view s, size_t* i) {
  while (*i < s.size()) {
    const unsigned char c = static_cast<unsigned char>(s[*i]);
    if (std::isspace(c)) {
      ++*i;
      continue;
    }
    if (c == '-' && *i + 1 < s.size() && s[*i + 1] == '-') {
      *i += 2;
      while (*i < s.size() && s[*i] != '\n' && s[*i] != '\r') {
        ++*i;
      }
      continue;
    }
    break;
  }
}

std::string ToUpper(std::string_view v) {
  std::string o;
  o.reserve(v.size());
  for (char ch : v) {
    o.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
  }
  return o;
}

bool ParseInt64(std::string_view s, int64_t* out) {
  const char* b = s.data();
  const char* e = s.data() + s.size();
  const std::from_chars_result r = std::from_chars(b, e, *out);
  return r.ec == std::errc{} && r.ptr == e;
}

bool ParseDouble(std::string_view s, double* out) {
  try {
    size_t idx = 0;
    *out = std::stod(std::string(s), &idx);
    return idx == s.size();
  } catch (...) {
    return false;
  }
}

}  // namespace

Parser::Token::Type Parser::ClassifyKeyword(std::string_view u) {
  if (u == "CREATE") {
    return Token::KW_CREATE;
  }
  if (u == "TABLE") {
    return Token::KW_TABLE;
  }
  if (u == "INSERT") {
    return Token::KW_INSERT;
  }
  if (u == "INTO") {
    return Token::KW_INTO;
  }
  if (u == "VALUES") {
    return Token::KW_VALUES;
  }
  if (u == "SELECT") {
    return Token::KW_SELECT;
  }
  if (u == "FROM") {
    return Token::KW_FROM;
  }
  if (u == "JOIN") {
    return Token::KW_JOIN;
  }
  if (u == "INNER") {
    return Token::KW_INNER;
  }
  if (u == "ON") {
    return Token::KW_ON;
  }
  if (u == "WHERE") {
    return Token::KW_WHERE;
  }
  if (u == "ORDER") {
    return Token::KW_ORDER;
  }
  if (u == "BY") {
    return Token::KW_BY;
  }
  if (u == "LIMIT") {
    return Token::KW_LIMIT;
  }
  if (u == "ASC") {
    return Token::KW_ASC;
  }
  if (u == "DESC") {
    return Token::KW_DESC;
  }
  if (u == "UPDATE") {
    return Token::KW_UPDATE;
  }
  if (u == "SET") {
    return Token::KW_SET;
  }
  if (u == "DELETE") {
    return Token::KW_DELETE;
  }
  if (u == "AND") {
    return Token::KW_AND;
  }
  if (u == "BEGIN") {
    return Token::KW_BEGIN;
  }
  if (u == "COMMIT") {
    return Token::KW_COMMIT;
  }
  if (u == "ROLLBACK") {
    return Token::KW_ROLLBACK;
  }
  if (u == "PRIMARY") {
    return Token::KW_PRIMARY;
  }
  if (u == "KEY") {
    return Token::KW_KEY;
  }
  if (u == "INT") {
    return Token::KW_INT;
  }
  if (u == "BIGINT") {
    return Token::KW_BIGINT;
  }
  if (u == "FLOAT") {
    return Token::KW_FLOAT;
  }
  if (u == "VARCHAR") {
    return Token::KW_VARCHAR;
  }
  if (u == "NOT") {
    return Token::KW_NOT;
  }
  if (u == "NULL") {
    return Token::KW_NULL;
  }
  if (u == "ABORT") {
    return Token::KW_ABORT;
  }
  if (u == "COUNT") {
    return Token::KW_COUNT;
  }
  if (u == "SUM") {
    return Token::KW_SUM;
  }
  if (u == "MIN") {
    return Token::KW_MIN;
  }
  if (u == "MAX") {
    return Token::KW_MAX;
  }
  return Token::IDENT;
}

std::variant<Statement, ParseError> Parser::Parse(std::string_view sql) {
  Parser p(sql);
  p.Tokenize();
  for (const Token& tk : p.tokens_) {
    if (tk.type == Token::UNKNOWN) {
      return ParseError{"lexer error near " + tk.value, tk.pos};
    }
  }
  if (p.AtEnd()) {
    return ParseError{"empty statement", 0};
  }
  try {
    Statement st = p.ParseStatement();
    if (!p.AtEnd()) {
      const Token t = p.Peek();
      if (t.type != Token::SEMICOLON) {
        return ParseError{"unexpected token after statement", t.pos};
      }
      p.Advance();
    }
    if (!p.AtEnd()) {
      const Token t = p.Peek();
      return ParseError{"trailing input after statement", t.pos};
    }
    return st;
  } catch (const ParseError& e) {
    return e;
  }
}

Parser::Parser(std::string_view sql) : input_(sql) {}

void Parser::Tokenize() {
  size_t i = 0;
  while (true) {
    SkipWsAndComments(input_, &i);
    if (i >= input_.size()) {
      tokens_.push_back(Token{Token::END_OF_INPUT, {}, i});
      return;
    }
    const size_t start = i;
    const char c = input_[i];

    if (IsIdentStart(c)) {
      while (i < input_.size() && IsIdentCont(input_[i])) {
        ++i;
      }
      const std::string raw(input_.substr(start, i - start));
      const std::string u = ToUpper(raw);
      const Token::Type ty = Parser::ClassifyKeyword(u);
      std::string stored = raw;
      if (ty == Token::IDENT) {
        stored.clear();
        stored.reserve(raw.size());
        for (unsigned char uch : raw) {
          stored.push_back(static_cast<char>(std::tolower(uch)));
        }
      }
      tokens_.push_back(Token{ty, stored, start});
      continue;
    }

    if (std::isdigit(static_cast<unsigned char>(c)) || (c == '.' && i + 1 < input_.size() &&
                                                        std::isdigit(static_cast<unsigned char>(
                                                            input_[i + 1])))) {
      bool is_float = false;
      size_t j = i;
      if (input_[j] == '.') {
        is_float = true;
      }
      while (j < input_.size() && std::isdigit(static_cast<unsigned char>(input_[j]))) {
        ++j;
      }
      if (j < input_.size() && input_[j] == '.') {
        is_float = true;
        ++j;
        while (j < input_.size() && std::isdigit(static_cast<unsigned char>(input_[j]))) {
          ++j;
        }
      }
      std::string num(input_.substr(start, j - start));
      i = j;
      tokens_.push_back(
          Token{is_float ? Token::NUMBER_FLOAT : Token::NUMBER_INT, std::move(num), start});
      continue;
    }

    if (c == '\'') {
      ++i;
      std::string lit;
      bool closed_string = false;
      while (i < input_.size()) {
        if (input_[i] == '\'') {
          if (i + 1 < input_.size() && input_[i + 1] == '\'') {
            lit.push_back('\'');
            i += 2;
            continue;
          }
          ++i;
          tokens_.push_back(Token{Token::STRING_LIT, std::move(lit), start});
          closed_string = true;
          break;
        }
        lit.push_back(input_[i]);
        ++i;
      }
      if (!closed_string) {
        tokens_.push_back(Token{Token::UNKNOWN, "'", start});
        return;
      }
      continue;
    }

    ++i;
    switch (c) {
      case '(':
        tokens_.push_back(Token{Token::LPAREN, "(", start});
        break;
      case ')':
        tokens_.push_back(Token{Token::RPAREN, ")", start});
        break;
      case ',':
        tokens_.push_back(Token{Token::COMMA, ",", start});
        break;
      case ';':
        tokens_.push_back(Token{Token::SEMICOLON, ";", start});
        break;
      case '*':
        tokens_.push_back(Token{Token::STAR, "*", start});
        break;
      case '+':
        tokens_.push_back(Token{Token::PLUS, "+", start});
        break;
      case '-':
        tokens_.push_back(Token{Token::MINUS, "-", start});
        break;
      case '.':
        tokens_.push_back(Token{Token::DOT, ".", start});
        break;
      case '=': {
        tokens_.push_back(Token{Token::EQ, "=", start});
        break;
      }
      case '!':
        if (i < input_.size() && input_[i] == '=') {
          ++i;
          tokens_.push_back(Token{Token::NE, "!=", start});
        } else {
          tokens_.push_back(Token{Token::UNKNOWN, "!", start});
          return;
        }
        break;
      case '<':
        if (i < input_.size() && input_[i] == '=') {
          ++i;
          tokens_.push_back(Token{Token::LE, "<=", start});
        } else if (i < input_.size() && input_[i] == '>') {
          ++i;
          tokens_.push_back(Token{Token::NE, "<>", start});
        } else {
          tokens_.push_back(Token{Token::LT, "<", start});
        }
        break;
      case '>':
        if (i < input_.size() && input_[i] == '=') {
          ++i;
          tokens_.push_back(Token{Token::GE, ">=", start});
        } else {
          tokens_.push_back(Token{Token::GT, ">", start});
        }
        break;
      default:
        tokens_.push_back(Token{Token::UNKNOWN, std::string(1, c), start});
        return;
    }
  }
}

Parser::Token Parser::Peek() const {
  return tokens_[std::min(pos_, tokens_.size() - 1)];
}

Parser::Token Parser::Advance() {
  Token t = Peek();
  if (!AtEnd()) {
    ++pos_;
  }
  return t;
}

bool Parser::Match(Token::Type type) {
  if (Peek().type == type) {
    Advance();
    return true;
  }
  return false;
}

Parser::Token Parser::Expect(Token::Type type) {
  const Token t = Peek();
  if (t.type != type) {
    throw ParseError{"unexpected token", t.pos};
  }
  Advance();
  return t;
}

bool Parser::AtEnd() const {
  return pos_ >= tokens_.size() - 1 ||
         Peek().type == Token::END_OF_INPUT || Peek().type == Token::UNKNOWN;
}

CreateTableStmt Parser::ParseCreateTable() {
  Expect(Token::KW_CREATE);
  Expect(Token::KW_TABLE);
  const Token name = Expect(Token::IDENT);
  Expect(Token::LPAREN);

  CreateTableStmt st;
  st.table_name = name.value;
  std::string pk_name;

  while (true) {
    const Token maybe_id = Peek();
    if (maybe_id.type != Token::IDENT) {
      throw ParseError{"expected column name", maybe_id.pos};
    }
    Advance();
    ColumnDef col;
    col.name = maybe_id.value;
    const Token ty = Peek();
    if (ty.type == Token::KW_INT) {
      Advance();
      col.type = ColumnType::INT;
    } else if (ty.type == Token::KW_BIGINT) {
      Advance();
      col.type = ColumnType::BIGINT;
    } else if (ty.type == Token::KW_FLOAT) {
      Advance();
      col.type = ColumnType::FLOAT;
    } else if (ty.type == Token::KW_VARCHAR) {
      Advance();
      if (Match(Token::LPAREN)) {
        Expect(Token::NUMBER_INT);
        Expect(Token::RPAREN);
      }
      col.type = ColumnType::VARCHAR;
    } else {
      throw ParseError{"expected column type", ty.pos};
    }

    if (Match(Token::KW_PRIMARY)) {
      Expect(Token::KW_KEY);
      pk_name = col.name;
    }
    st.columns.push_back(std::move(col));
    if (Match(Token::RPAREN)) {
      break;
    }
    Expect(Token::COMMA);
  }

  if (pk_name.empty()) {
    throw ParseError{"PRIMARY KEY required", name.pos};
  }
  st.primary_key = pk_name;
  return st;
}

InsertStmt Parser::ParseInsert() {
  Expect(Token::KW_INSERT);
  Expect(Token::KW_INTO);
  const Token tn = Expect(Token::IDENT);
  Expect(Token::LPAREN);
  InsertStmt st;
  st.table_name = tn.value;
  st.columns.push_back(Expect(Token::IDENT).value);
  while (Match(Token::COMMA)) {
    st.columns.push_back(Expect(Token::IDENT).value);
  }
  Expect(Token::RPAREN);
  Expect(Token::KW_VALUES);
  Expect(Token::LPAREN);
  st.values.push_back(ParseLiteral());
  while (Match(Token::COMMA)) {
    st.values.push_back(ParseLiteral());
  }
  Expect(Token::RPAREN);
  if (st.columns.size() != st.values.size()) {
    throw ParseError{"column/value count mismatch", tn.pos};
  }
  return st;
}

WhereClause Parser::ParseWhere() {
  WhereClause w;
  w.conditions.push_back(ParseCompare());
  while (Match(Token::KW_AND)) {
    w.conditions.push_back(ParseCompare());
  }
  return w;
}

CompareExpr Parser::ParseCompare() {
  CompareExpr ex;
  ex.column.name = ParseColumnRef();
  const Token pop = Peek();
  if (pop.type == Token::EQ) {
    Advance();
    ex.op = CompareOp::EQ;
  } else if (pop.type == Token::NE) {
    Advance();
    ex.op = CompareOp::NE;
  } else if (pop.type == Token::LT) {
    Advance();
    ex.op = CompareOp::LT;
  } else if (pop.type == Token::LE) {
    Advance();
    ex.op = CompareOp::LE;
  } else if (pop.type == Token::GT) {
    Advance();
    ex.op = CompareOp::GT;
  } else if (pop.type == Token::GE) {
    Advance();
    ex.op = CompareOp::GE;
  } else {
    throw ParseError{"expected comparison operator", pop.pos};
  }
  ex.value = ParseLiteral();
  return ex;
}

std::string Parser::ParseColumnRef() {
  std::string out = Expect(Token::IDENT).value;
  while (Match(Token::DOT)) {
    out.append(".");
    out.append(Expect(Token::IDENT).value);
  }
  return out;
}

LiteralExpr Parser::ParseLiteral() {
  LiteralExpr lit;
  const Token t = Peek();
  if (t.type == Token::NUMBER_INT) {
    Advance();
    int64_t v = 0;
    if (!ParseInt64(t.value, &v)) {
      throw ParseError{"bad integer literal", t.pos};
    }
    lit.value = v;
    return lit;
  }
  if (t.type == Token::NUMBER_FLOAT) {
    Advance();
    double v = 0;
    if (!ParseDouble(t.value, &v)) {
      throw ParseError{"bad float literal", t.pos};
    }
    lit.value = v;
    return lit;
  }
  if (t.type == Token::STRING_LIT) {
    Advance();
    lit.value = t.value;
    return lit;
  }
  throw ParseError{"expected literal", t.pos};
}

SelectStmt Parser::ParseSelect() {
  Expect(Token::KW_SELECT);
  SelectStmt st;
  if (Match(Token::STAR)) {
    st.select_all = true;
  } else {
    auto parse_select_item = [&]() {
      Token t = Peek();
      if (t.type == Token::KW_COUNT || t.type == Token::KW_SUM || t.type == Token::KW_MIN ||
          t.type == Token::KW_MAX) {
        Advance();
        SelectStmt::Aggregate agg;
        if (t.type == Token::KW_COUNT) {
          agg.func = SelectStmt::Aggregate::Func::COUNT;
        } else if (t.type == Token::KW_SUM) {
          agg.func = SelectStmt::Aggregate::Func::SUM;
        } else if (t.type == Token::KW_MIN) {
          agg.func = SelectStmt::Aggregate::Func::MIN;
        } else {
          agg.func = SelectStmt::Aggregate::Func::MAX;
        }
        Expect(Token::LPAREN);
        if (Match(Token::STAR)) {
          agg.star = true;
        } else {
          agg.column = ParseColumnRef();
        }
        Expect(Token::RPAREN);
        st.aggregates.push_back(std::move(agg));
      } else {
        st.columns.push_back(ParseColumnRef());
      }
    };
    parse_select_item();
    while (Match(Token::COMMA)) {
      parse_select_item();
    }
  }
  Expect(Token::KW_FROM);
  st.table_name = ParseColumnRef();
  if (Match(Token::KW_INNER)) {
    Expect(Token::KW_JOIN);
    SelectStmt::JoinClause jc;
    jc.table_name = ParseColumnRef();
    Expect(Token::KW_ON);
    jc.left_column = ParseColumnRef();
    Expect(Token::EQ);
    jc.right_column = ParseColumnRef();
    st.join = std::move(jc);
  } else if (Match(Token::KW_JOIN)) {
    SelectStmt::JoinClause jc;
    jc.table_name = ParseColumnRef();
    Expect(Token::KW_ON);
    jc.left_column = ParseColumnRef();
    Expect(Token::EQ);
    jc.right_column = ParseColumnRef();
    st.join = std::move(jc);
  }
  if (Match(Token::KW_WHERE)) {
    st.where = ParseWhere();
    st.has_where = true;
  }
  if (Match(Token::KW_ORDER)) {
    Expect(Token::KW_BY);
    st.order_by_column = ParseColumnRef();
    st.has_order_by = true;
    if (Match(Token::KW_DESC)) {
      st.order_desc = true;
    } else {
      (void)Match(Token::KW_ASC);
    }
  }
  if (Match(Token::KW_LIMIT)) {
    Token lim = Expect(Token::NUMBER_INT);
    int64_t v = 0;
    if (!ParseInt64(lim.value, &v) || v < 0) {
      throw ParseError{"expected non-negative LIMIT integer", lim.pos};
    }
    st.limit = static_cast<uint64_t>(v);
    st.has_limit = true;
  }
  return st;
}

UpdateStmt Parser::ParseUpdate() {
  Expect(Token::KW_UPDATE);
  UpdateStmt st;
  st.table_name = Expect(Token::IDENT).value;
  Expect(Token::KW_SET);

  while (true) {
    UpdateStmt::Assignment as;
    as.column = Expect(Token::IDENT).value;
    Expect(Token::EQ);
    const Token first = Peek();
    if (first.type != Token::IDENT) {
      as.literal = ParseLiteral();
    } else {
      // Could be ident + arith; copy rule: ident ('+'|'-') literal
      Token id = Advance();
      const Token op = Peek();
      if (op.type == Token::PLUS || op.type == Token::MINUS) {
        Advance();
        ArithExpr ax;
        ax.column.name = id.value;
        ax.op = (op.type == Token::PLUS) ? ArithOp::ADD : ArithOp::SUB;
        ax.operand = ParseLiteral();
        as.arith = std::move(ax);
      } else {
        // ident alone as literal assignment? Not in grammar. Treat as error.
        throw ParseError{"expected operator after column in SET", op.pos};
      }
    }
    st.assignments.push_back(std::move(as));
    if (!Match(Token::COMMA)) {
      break;
    }
  }

  Expect(Token::KW_WHERE);
  st.where = ParseWhere();
  return st;
}

DeleteStmt Parser::ParseDelete() {
  Expect(Token::KW_DELETE);
  Expect(Token::KW_FROM);
  DeleteStmt st;
  st.table_name = Expect(Token::IDENT).value;
  Expect(Token::KW_WHERE);
  st.where = ParseWhere();
  return st;
}

Statement Parser::ParseStatement() {
  const Token t = Peek();
  switch (t.type) {
    case Token::KW_CREATE:
      return ParseCreateTable();
    case Token::KW_INSERT:
      return ParseInsert();
    case Token::KW_SELECT:
      return ParseSelect();
    case Token::KW_UPDATE:
      return ParseUpdate();
    case Token::KW_DELETE:
      return ParseDelete();
    case Token::KW_BEGIN:
      Advance();
      return BeginStmt{};
    case Token::KW_COMMIT:
      Advance();
      return CommitStmt{};
    case Token::KW_ROLLBACK:
    case Token::KW_ABORT:
      Advance();
      return RollbackStmt{};
    default:
      throw ParseError{"expected statement", t.pos};
  }
}

}  // namespace txndb
