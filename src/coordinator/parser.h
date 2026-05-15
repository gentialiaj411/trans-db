#pragma once

#include "coordinator/catalog.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace txndb {

// --- Expressions (for WHERE clauses and SET values) ---

// A literal value
struct LiteralExpr {
  std::variant<int64_t, double, std::string> value;
};

// A column reference
struct ColumnRef {
  std::string name;
};

// Binary comparison: column op literal
enum class CompareOp { EQ, NE, LT, LE, GT, GE };

struct CompareExpr {
  ColumnRef column;
  CompareOp op;
  LiteralExpr value;
};

// expr + expr, expr - expr (for UPDATE SET col = col + value)
enum class ArithOp { ADD, SUB };

struct ArithExpr {
  ColumnRef column;
  ArithOp op;
  LiteralExpr operand;
};

// WHERE clause: one or more CompareExprs joined by AND
struct WhereClause {
  std::vector<CompareExpr> conditions;  // all ANDed together
};

// --- Statements ---

struct CreateTableStmt {
  std::string table_name;
  std::vector<ColumnDef> columns;
  std::string primary_key;
};

struct InsertStmt {
  std::string table_name;
  std::vector<std::string> columns;    // column names in INSERT (col1, col2, ...)
  std::vector<LiteralExpr> values;    // VALUES (v1, v2, ...)
};

struct SelectStmt {
  std::string table_name;
  std::vector<std::string> columns;  // empty = SELECT *
  WhereClause where;
};

struct UpdateStmt {
  std::string table_name;
  // SET assignments: col = literal or col = col +/- literal
  struct Assignment {
    std::string column;
    std::optional<ArithExpr> arith;      // col = col + val
    std::optional<LiteralExpr> literal;  // col = val (one of arith/literal is set)
  };
  std::vector<Assignment> assignments;
  WhereClause where;
};

struct DeleteStmt {
  std::string table_name;
  WhereClause where;
};

struct BeginStmt {};
struct CommitStmt {};
struct RollbackStmt {};

using Statement =
    std::variant<CreateTableStmt, InsertStmt, SelectStmt, UpdateStmt, DeleteStmt, BeginStmt,
                 CommitStmt, RollbackStmt>;

// --- Parser ---

struct ParseError {
  std::string message;
  size_t position;  // byte offset in input where error occurred
};

class Parser {
public:
  // Parse a single SQL statement. Returns the statement or an error.
  static std::variant<Statement, ParseError> Parse(std::string_view sql);

private:
  explicit Parser(std::string_view sql);

  // Tokenizer
  struct Token {
    enum Type {
      IDENT,
      NUMBER_INT,
      NUMBER_FLOAT,
      STRING_LIT,
      LPAREN,
      RPAREN,
      COMMA,
      SEMICOLON,
      STAR,
      PLUS,
      MINUS,
      DOT,
      EQ,
      NE,
      LT,
      LE,
      GT,
      GE,
      KW_CREATE,
      KW_TABLE,
      KW_INSERT,
      KW_INTO,
      KW_VALUES,
      KW_SELECT,
      KW_FROM,
      KW_WHERE,
      KW_UPDATE,
      KW_SET,
      KW_DELETE,
      KW_AND,
      KW_BEGIN,
      KW_COMMIT,
      KW_ROLLBACK,
      KW_PRIMARY,
      KW_KEY,
      KW_INT,
      KW_BIGINT,
      KW_FLOAT,
      KW_VARCHAR,
      KW_NOT,
      KW_NULL,
      KW_ABORT,
      END_OF_INPUT,
      UNKNOWN,
    } type;
    std::string value;  // raw text of the token
    size_t pos;          // byte offset
  };

  void Tokenize();

  static Token::Type ClassifyKeyword(std::string_view u);

  Token Peek() const;
  Token Advance();
  bool Match(Token::Type type);
  Token Expect(Token::Type type);
  bool AtEnd() const;

  // Grammar rules
  Statement ParseStatement();
  CreateTableStmt ParseCreateTable();
  InsertStmt ParseInsert();
  SelectStmt ParseSelect();
  UpdateStmt ParseUpdate();
  DeleteStmt ParseDelete();
  WhereClause ParseWhere();
  CompareExpr ParseCompare();
  LiteralExpr ParseLiteral();

  std::string_view input_;
  std::vector<Token> tokens_;
  size_t pos_{0};
};

}  // namespace txndb
