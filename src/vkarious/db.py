"""Database connection helpers for vkarious."""

from __future__ import annotations

import psycopg


def connect(dsn: str) -> psycopg.Connection:
    """Return a new PostgreSQL connection using the provided DSN."""
    return psycopg.connect(dsn)
