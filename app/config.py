"""Runtime configuration (environment variables)."""
from __future__ import annotations

import os

# PostgreSQL DSN, e.g. postgresql://postgres:postgres@db:5432/diag
DATABASE_URL: str = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/diag"
)

# The service reuses a FIXED number of connections (spec: exactly two).
POOL_SIZE: int = 2

# Max statements per job (spec: at most 10).
MAX_STATEMENTS: int = 10

# Safety caps.
MAX_STATEMENT_CHARS: int = int(os.environ.get("MAX_STATEMENT_CHARS", "100000"))
ROW_LIMIT: int = int(os.environ.get("ROW_LIMIT", "200"))
MAX_QUEUED_JOBS: int = int(os.environ.get("MAX_QUEUED_JOBS", "1000"))
JOB_HISTORY: int = int(os.environ.get("JOB_HISTORY", "1000"))

# Test hooks (global defaults; per-request overrides take precedence).
# CANCEL_SEND_DELAY_MS: wait this long before the cancel is actually sent to
#   PostgreSQL (simulates a slow cancel pipeline; the API still accepts the
#   cancel immediately).
# CLEANUP_DELAY_MS: pause inside the session-cleanup phase (simulates slow
#   cleanup; the other connection is unaffected).
CANCEL_SEND_DELAY_MS: int = int(os.environ.get("CANCEL_SEND_DELAY_MS", "0"))
CLEANUP_DELAY_MS: int = int(os.environ.get("CLEANUP_DELAY_MS", "0"))

# Upper bound for the per-request delay hooks above.
MAX_HOOK_DELAY_MS: int = int(os.environ.get("MAX_HOOK_DELAY_MS", "120000"))

# How long a worker waits for an in-flight cancel send before it may return
# the connection. Must exceed MAX_HOOK_DELAY_MS.
CANCEL_SEND_WAIT_S: float = float(os.environ.get("CANCEL_SEND_WAIT_S", "180"))

CONNECT_TIMEOUT_S: int = int(os.environ.get("CONNECT_TIMEOUT_S", "10"))
