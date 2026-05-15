#include <variant>

#include <gtest/gtest.h>

#include "coordinator/parser.h"

using namespace txndb;

Statement MustParseOk(std::string_view sql) {
  auto pv = Parser::Parse(sql);
  auto* bad = std::get_if<ParseError>(&pv);
  if (bad) {
    ADD_FAILURE() << "unexpected parse error: " << bad->message;
    return BeginStmt{};
  }
  return std::get<Statement>(std::move(pv));
}

TEST(ParserTest, ParseCreateTable) {
  const char* sql = "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR)";
  const auto pv = Parser::Parse(sql);
  ASSERT_FALSE(std::holds_alternative<ParseError>(pv));
  const Statement& stmt = std::get<Statement>(pv);
  const auto* ct = std::get_if<CreateTableStmt>(&stmt);
  ASSERT_NE(ct, nullptr);
  EXPECT_EQ(ct->table_name, "t");
  ASSERT_EQ(ct->columns.size(), 2u);
  EXPECT_EQ(ct->columns[0].name, "id");
  EXPECT_EQ(ct->columns[0].type, ColumnType::INT);
  EXPECT_EQ(ct->columns[1].name, "name");
  EXPECT_EQ(ct->columns[1].type, ColumnType::VARCHAR);
  EXPECT_EQ(ct->primary_key, "id");
}

TEST(ParserTest, ParseInsert) {
  auto st =
      MustParseOk("INSERT INTO t (id, name) VALUES (1, 'hello')");
  const auto* i = std::get_if<InsertStmt>(&st);
  ASSERT_NE(i, nullptr);
  ASSERT_EQ(i->columns.size(), 2u);
  EXPECT_EQ(i->columns[0], "id");
  EXPECT_EQ(i->columns[1], "name");
  ASSERT_EQ(i->values.size(), 2u);
  ASSERT_TRUE(std::holds_alternative<int64_t>(i->values[0].value));
  EXPECT_EQ(std::get<int64_t>(i->values[0].value), 1);
  ASSERT_TRUE(std::holds_alternative<std::string>(i->values[1].value));
  EXPECT_EQ(std::get<std::string>(i->values[1].value), "hello");
}

TEST(ParserTest, ParseSelectStar) {
  auto st = MustParseOk("SELECT * FROM t WHERE id = 1;");
  const auto* s = std::get_if<SelectStmt>(&st);
  ASSERT_NE(s, nullptr);
  EXPECT_EQ(s->table_name, "t");
  ASSERT_TRUE(s->columns.empty());
  ASSERT_EQ(s->where.conditions.size(), 1u);
  EXPECT_EQ(s->where.conditions[0].column.name, "id");
  EXPECT_EQ(s->where.conditions[0].op, CompareOp::EQ);
  ASSERT_TRUE(std::holds_alternative<int64_t>(s->where.conditions[0].value.value));
  EXPECT_EQ(std::get<int64_t>(s->where.conditions[0].value.value), 1);
}

TEST(ParserTest, ParseSelectColumns) {
  auto st = MustParseOk("SELECT id, name FROM t WHERE id = 1");
  const auto* s = std::get_if<SelectStmt>(&st);
  ASSERT_NE(s, nullptr);
  ASSERT_EQ(s->columns.size(), 2u);
  EXPECT_EQ(s->columns[0], "id");
}

TEST(ParserTest, ParseUpdate) {
  auto st = MustParseOk("UPDATE t SET name = 'new' WHERE id = 1");
  const auto* u = std::get_if<UpdateStmt>(&st);
  ASSERT_NE(u, nullptr);
  ASSERT_EQ(u->assignments.size(), 1u);
  EXPECT_EQ(u->assignments[0].column, "name");
  ASSERT_TRUE(u->assignments[0].literal.has_value());
  EXPECT_FALSE(u->assignments[0].arith.has_value());
}

TEST(ParserTest, ParseUpdateArith) {
  auto st = MustParseOk("UPDATE t SET balance = balance + 100 WHERE id = 1");
  const auto* u = std::get_if<UpdateStmt>(&st);
  ASSERT_NE(u, nullptr);
  ASSERT_FALSE(u->assignments.empty());
  ASSERT_TRUE(u->assignments[0].arith.has_value());
  EXPECT_EQ(u->assignments[0].arith->op, ArithOp::ADD);
  EXPECT_EQ(u->assignments[0].arith->column.name, "balance");
  ASSERT_FALSE(u->assignments[0].literal.has_value());
}

TEST(ParserTest, ParseDelete) {
  auto st = MustParseOk("DELETE FROM t WHERE id = 1");
  const auto* d = std::get_if<DeleteStmt>(&st);
  ASSERT_NE(d, nullptr);
  ASSERT_EQ(d->where.conditions.size(), 1u);
}

TEST(ParserTest, ParseBeginCommitRollback) {
  EXPECT_TRUE(std::holds_alternative<BeginStmt>(MustParseOk("BEGIN")));
  EXPECT_TRUE(std::holds_alternative<CommitStmt>(MustParseOk("COMMIT")));
  EXPECT_TRUE(std::holds_alternative<RollbackStmt>(MustParseOk("ROLLBACK")));
}

TEST(ParserTest, ParseCaseInsensitive) {
  auto up = MustParseOk("SELECT * FROM T WHERE ID = 1");
  auto low = MustParseOk("select * from t where id = 1");

  const auto* su = std::get_if<SelectStmt>(&up);
  const auto* sl = std::get_if<SelectStmt>(&low);
  ASSERT_NE(su, nullptr);
  ASSERT_NE(sl, nullptr);
  EXPECT_EQ(su->table_name, sl->table_name);
  EXPECT_EQ(su->columns.size(), sl->columns.size());
  ASSERT_EQ(sl->where.conditions[0].column.name, "id");
}

TEST(ParserTest, ParseStringEscape) {
  auto st =
      MustParseOk("INSERT INTO t (id, name) VALUES (1, 'it''s')");
  const auto* i = std::get_if<InsertStmt>(&st);
  ASSERT_NE(i, nullptr);
  ASSERT_GE(i->values.size(), 2u);
  ASSERT_TRUE(std::holds_alternative<std::string>(i->values[1].value));
  EXPECT_EQ(std::get<std::string>(i->values[1].value), "it's");
}

TEST(ParserTest, ParseError) {
  auto pv = Parser::Parse("SELEC * FROM t");
  EXPECT_TRUE(std::holds_alternative<ParseError>(pv));
}

TEST(ParserTest, ParseMultiConditionWhere) {
  auto st = MustParseOk("SELECT * FROM t WHERE id >= 10 AND id < 20");
  const auto* s = std::get_if<SelectStmt>(&st);
  ASSERT_NE(s, nullptr);
  ASSERT_EQ(s->where.conditions.size(), 2u);
  EXPECT_EQ(s->where.conditions[0].column.name, "id");
  EXPECT_EQ(s->where.conditions[0].op, CompareOp::GE);
  EXPECT_EQ(s->where.conditions[1].column.name, "id");
  EXPECT_EQ(s->where.conditions[1].op, CompareOp::LT);
}
