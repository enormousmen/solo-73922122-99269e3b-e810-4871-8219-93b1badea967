"""Unit tests for the SQL guard (no database needed)."""
import pytest

from app.sqlguard import SqlRejected, check_statements


def ok(stmts):
    return check_statements(stmts)


class TestSingleStatement:
    def test_simple(self):
        assert ok(["SELECT 1"]) == ["SELECT 1"]

    def test_trailing_semicolon_is_one_statement(self):
        assert ok(["SELECT 1;"]) == ["SELECT 1;"]

    def test_multiple_statements_rejected(self):
        with pytest.raises(SqlRejected, match="2 statements"):
            ok(["SELECT 1; SELECT 2"])

    def test_semicolon_in_string_literal(self):
        assert ok(["SELECT ';'"]) == ["SELECT ';'"]

    def test_semicolon_in_quoted_identifier(self):
        assert ok(['SELECT 1 AS "a;b"']) == ['SELECT 1 AS "a;b"']

    def test_semicolon_in_dollar_quote(self):
        assert ok(["SELECT $$a;b$$"]) == ["SELECT $$a;b$$"]

    def test_dollar_quoted_function_body(self):
        body = "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $body$ BEGIN RAISE NOTICE 'x;y'; END; $body$"
        assert ok([body]) == [body]

    def test_tagged_dollar_quote(self):
        assert ok(["SELECT $tag$x;y$tag$"]) == ["SELECT $tag$x;y$tag$"]

    def test_semicolon_in_line_comment(self):
        assert ok(["SELECT 1 -- a;b\n"]) == ["SELECT 1 -- a;b"]

    def test_semicolon_in_block_comment(self):
        assert ok(["/* a;b /* nested ; */ c */ SELECT 1"]) == [
            "/* a;b /* nested ; */ c */ SELECT 1"
        ]

    def test_empty_rejected(self):
        with pytest.raises(SqlRejected, match="empty"):
            ok(["   "])

    def test_comment_only_rejected(self):
        with pytest.raises(SqlRejected, match="empty"):
            ok(["-- nothing here"])

    def test_semicolons_only_rejected(self):
        with pytest.raises(SqlRejected, match="empty"):
            ok([";;;"])

    def test_too_many_statements(self):
        with pytest.raises(SqlRejected, match="too many"):
            ok(["SELECT 1"] * 11)

    def test_ten_statements_ok(self):
        assert len(ok(["SELECT 1"] * 10)) == 10

    def test_whitespace_stripped(self):
        assert ok(["  SELECT 1  "]) == ["SELECT 1"]


class TestTransactionControl:
    @pytest.mark.parametrize(
        "sql",
        [
            "BEGIN",
            "BEGIN;",
            "begin",
            "BEGIN TRANSACTION",
            "START TRANSACTION",
            "COMMIT",
            "COMMIT PREPARED 'x'",
            "ROLLBACK",
            "ROLLBACK TO SAVEPOINT s",
            "ABORT",
            "END",
            "SAVEPOINT s",
            "RELEASE SAVEPOINT s",
            "RELEASE s",
            "SET TRANSACTION READ ONLY",
            "set transaction isolation level serializable",
            "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
            "PREPARE TRANSACTION 'gid'",
            "/* sneaky */ BEGIN",
            "-- c\nBEGIN",
        ],
    )
    def test_rejected(self, sql):
        with pytest.raises(SqlRejected, match="transaction control|TRANSACTION"):
            ok([sql])

    @pytest.mark.parametrize(
        "sql",
        [
            "SET work_mem = '64MB'",
            "SET LOCAL work_mem = '64MB'",
            "SET SESSION work_mem = '64MB'",
            "SET application_name = 'diag'",
            "RESET ALL",
            "PREPARE p AS SELECT 1",
            "EXECUTE p",
            "DEALLOCATE ALL",
            "SELECT 1",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "CREATE TEMP TABLE t (a int)",
            "SELECT pg_advisory_lock(1)",
            "VACUUM",
            "CHECKPOINT",
            "DISCARD ALL",
            "DO $$ BEGIN RAISE NOTICE 'hi'; END $$",
        ],
    )
    def test_allowed(self, sql):
        assert ok([sql]) == [sql]
