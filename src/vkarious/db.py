"""Database connection helpers for vkarious."""

from __future__ import annotations

import os
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

import psycopg


def get_database_dsn() -> str:
    """Get the database DSN from VKA_DATABASE environment variable."""
    dsn = os.getenv("VKA_DATABASE")
    if not dsn:
        raise ValueError("VKA_DATABASE environment variable is required")
    return dsn


def connect(dsn: str | None = None) -> psycopg.Connection:
    """Return a new PostgreSQL connection using the provided DSN or VKA_DATABASE."""
    if dsn is None:
        dsn = get_database_dsn()
    return psycopg.connect(dsn)


def list_databases() -> list[dict[str, str | int]]:
    """List all databases with their OIDs and names."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT oid, datname FROM pg_database ORDER BY datname")
            return [{"oid": row[0], "name": row[1]} for row in cur.fetchall()]


def get_data_directory() -> str:
    """Get the PostgreSQL data directory path."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW data_directory")
            return cur.fetchone()[0]


def get_database_oid(database_name: str) -> int:
    """Get the OID of a specific database."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT oid FROM pg_database WHERE datname = %s", (database_name,))
            result = cur.fetchone()
            if result is None:
                raise ValueError(f"Database '{database_name}' not found")
            return result[0]


def terminate_database_connections(database_name: str) -> int:
    """Terminate all connections to a database except the current one."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
            """, (database_name,))
            terminated_count = cur.rowcount
            conn.commit()
            return terminated_count


@contextmanager
def database_write_lock(database_name: str) -> Iterator[None]:
    """Context manager to acquire an exclusive lock on a database to prevent writes."""
    # Connect to the specific database to lock it
    db_dsn = get_database_dsn()
    # Parse DSN and modify it to connect to the target database
    conn_params = psycopg.conninfo.conninfo_to_dict(db_dsn)
    conn_params['dbname'] = database_name
    target_dsn = psycopg.conninfo.make_conninfo(**conn_params)
    
    conn = psycopg.connect(target_dsn)
    try:
        with conn.cursor() as cur:
            # First terminate existing connections
            # terminate_database_connections(database_name)
            print("checkpoint")
            cur.execute("CHECKPOINT")

            time.sleep(1)  # Brief pause to ensure connections are terminated
            
            # Acquire an advisory lock
            cur.execute("SELECT pg_advisory_lock(12345)")
            conn.commit()
            
        yield
    finally:
        with conn.cursor() as cur:
            # Release the advisory lock
            cur.execute("SELECT pg_advisory_unlock(12345)")
            conn.commit()
        conn.close()


def create_snapshot_database(source_database: str) -> tuple[str, int]:
    """Create a new database for snapshot with timestamp name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_name = f"snapshot_{source_database}_{timestamp}"
    
    with connect() as conn:
        # Set autocommit for CREATE DATABASE (required for PostgreSQL)
        conn.autocommit = True
        with conn.cursor() as cur:
            # Create the new database
            cur.execute(f'''CREATE DATABASE "{snapshot_name}" STRATEGY='FILE_COPY' ''')
            
            # Get the OID of the newly created database
            cur.execute("SELECT oid FROM pg_database WHERE datname = %s", (snapshot_name,))
            oid = cur.fetchone()[0]
            
    return snapshot_name, oid


def copy_database_files(data_directory: str, source_oid: int, target_oid: int) -> None:
    """Copy database files from source to target using OIDs."""
    data_path = Path(data_directory)
    base_path = data_path / "base"
    
    source_path = base_path / str(source_oid)
    target_path = base_path / str(target_oid)
    
    if not source_path.exists():
        raise FileNotFoundError(f"Source database directory not found: {source_path}")
    
    if not target_path.exists():
        raise FileNotFoundError(f"Target database directory not found: {target_path}")
    
    # Use cp -cR for copy-on-write if available (macOS/BSD), fallback to cp -r
    try:
        subprocess.run(
            ["rm", "-rf", str(target_path) + "/"],
            check=True,
            capture_output=True,
            text=True
        )


        subprocess.run(
            ["cp", "-cR", str(source_path) + "/", str(target_path) + "/"],
            check=True,
            capture_output=True,
            text=True
        )
    except subprocess.CalledProcessError:
        raise
        # Fallback to regular recursive copy if cp -c is not available
        # subprocess.run(
        #     ["cp", "-r", str(source_path) + "/", str(target_path) + "/"],
        #     check=True,
        #     capture_output=True,
        #     text=True
        # )
    
    # Remove pg_internal.init file from the copied directory
    pg_internal_init = target_path / "pg_internal.init"
    if pg_internal_init.exists():
        pg_internal_init.unlink()
    else:
        print("no internal file ?")
