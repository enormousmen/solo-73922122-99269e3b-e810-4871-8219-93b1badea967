"""Static validation of submitted SQL.

Two guard rules are enforced before a job is accepted:

1. Every list element must contain exactly one SQL statement.  Semicolons
   inside string literals, quoted identifiers, dollar-quoted bodies and
   comments do not count as separators.
2. Explicit transaction control is rejected.  Jobs run in autocommit mode
   (each statement commits on its own); BEGIN/COMMIT/ROLLBACK/SAVEPOINT/...
   would break that contract and defeat the session-cleanup guarantees.

Note: callers submit *trusted* SQL (on-call DBAs).  This module is not a
security boundary; it only enforces the execution contract above.
"""
from __future__ import annotations

from . import config

__all__ = ["SqlRejected", "check_statements"]


class SqlRejected(ValueError):
    """Raised when a submitted statement violates a guard rule."""


def _scan(sql: str):
    """Yield ("word" | "punct" | "sep", value) for top-level tokens.

    Comments, string literals, quoted identifiers and dollar-quoted bodies
    are skipped: they can never contain a top-level ``;`` separator or a
    leading transaction keyword.
    """
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c.isspace():
            i += 1
            continue
        # line comment
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j + 1
            continue
        # block comment (PostgreSQL nests them)
        if sql.startswith("/*", i):
            depth = 1
            i += 2
            while i < n and depth:
                if sql.startswith("/*", i):
                    depth += 1
                    i += 2
                elif sql.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            continue
        # single-quoted string, '' escape (prefixes like E'...' emit a
        # harmless leading word token first)
        if c == "'":
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        # quoted identifier, "" escape
        if c == '"':
            i += 1
            while i < n:
                if sql[i] == '"':
                    if i + 1 < n and sql[i + 1] == '"':
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        # dollar-quoted body $tag$ ... $tag$
        if c == "$":
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            if j < n and sql[j] == "$":
                tag = sql[i : j + 1]
                end = sql.find(tag, j + 1)
                i = n if end < 0 else end + len(tag)
                continue
            yield ("punct", "$")
            i += 1
            continue
        if c.isalpha() or c == "_":
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] in "_$"):
                j += 1
            yield ("word", sql[i:j].upper())
            i = j
            continue
        if c == ";":
            yield ("sep", ";")
            i += 1
            continue
        yield ("punct", c)
        i += 1


# Leading keywords that are always explicit transaction control.
_TX_KEYWORDS = {
    "BEGIN",
    "START",  # START TRANSACTION (START REPLICATION is rejected too, fine)
    "COMMIT",  # also COMMIT PREPARED
    "ROLLBACK",  # also ROLLBACK PREPARED / ROLLBACK TO SAVEPOINT
    "ABORT",
    "END",
    "SAVEPOINT",
    "RELEASE",  # RELEASE SAVEPOINT
}


def _check_tx_control(words: list[str], pos: int) -> None:
    if not words:
        return
    w0 = words[0]
    if w0 in _TX_KEYWORDS:
        raise SqlRejected(
            f"statement {pos}: explicit transaction control ({w0} ...) is not "
            "allowed; every statement is auto-committed on execution"
        )
    if w0 == "SET":
        if len(words) > 1 and words[1] == "TRANSACTION":
            raise SqlRejected(
                f"statement {pos}: SET TRANSACTION is not allowed "
                "(explicit transaction control)"
            )
        if len(words) > 2 and words[1] == "SESSION" and words[2] == "CHARACTERISTICS":
            raise SqlRejected(
                f"statement {pos}: SET SESSION CHARACTERISTICS is not allowed "
                "(explicit transaction control)"
            )
    if w0 == "PREPARE" and len(words) > 1 and words[1] == "TRANSACTION":
        raise SqlRejected(
            f"statement {pos}: PREPARE TRANSACTION is not allowed "
            "(explicit transaction control)"
        )


def check_statements(statements: list[str]) -> list[str]:
    """Validate a submitted statement list; return the stripped statements.

    Raises SqlRejected on any violation.
    """
    if not statements:
        raise SqlRejected("no statements submitted")
    if len(statements) > config.MAX_STATEMENTS:
        raise SqlRejected(
            f"too many statements: {len(statements)} > {config.MAX_STATEMENTS}"
        )
    out: list[str] = []
    for pos, raw in enumerate(statements, 1):
        if not isinstance(raw, str):
            raise SqlRejected(f"statement {pos}: must be a string")
        sql = raw.strip()
        if not sql:
            raise SqlRejected(f"statement {pos}: empty statement")
        if len(sql) > config.MAX_STATEMENT_CHARS:
            raise SqlRejected(
                f"statement {pos}: too long "
                f"({len(sql)} > {config.MAX_STATEMENT_CHARS} chars)"
            )
        # split into top-level segments on ';'
        segments: list[list[tuple[str, str]]] = [[]]
        for kind, value in _scan(sql):
            if kind == "sep":
                segments.append([])
            else:
                segments[-1].append((kind, value))
        non_empty = [seg for seg in segments if seg]
        if not non_empty:
            raise SqlRejected(
                f"statement {pos}: empty statement (only comments/semicolons)"
            )
        if len(non_empty) > 1:
            raise SqlRejected(
                f"statement {pos}: contains {len(non_empty)} statements; "
                "send each statement as a separate list element"
            )
        words = [v for k, v in non_empty[0] if k == "word"]
        _check_tx_control(words, pos)
        out.append(sql)
    return out
