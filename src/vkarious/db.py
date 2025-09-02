"""Database connection helpers for vkarious."""

from __future__ import annotations

import os
import re
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


def database_exists(database_name: str) -> bool:
    """Check if a database exists."""
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
                return cur.fetchone() is not None
    except Exception:
        return False


def create_database(database_name: str) -> None:
    """Create a database."""
    with connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{database_name}"')


def table_exists(table_name: str, database_name: str = "vkarious") -> bool:
    """Check if a table exists in the specified database."""
    db_dsn = get_database_dsn()
    conn_params = psycopg.conninfo.conninfo_to_dict(db_dsn)
    conn_params['dbname'] = database_name
    target_dsn = psycopg.conninfo.make_conninfo(**conn_params)
    
    try:
        with psycopg.connect(target_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = %s AND table_schema = 'public'
                """, (table_name,))
                return cur.fetchone() is not None
    except Exception:
        return False


def get_current_version(database_name: str = "vkarious") -> str:
    """Get the current migration version from vka_dbversion table."""
    db_dsn = get_database_dsn()
    conn_params = psycopg.conninfo.conninfo_to_dict(db_dsn)
    conn_params['dbname'] = database_name
    target_dsn = psycopg.conninfo.make_conninfo(**conn_params)
    
    try:
        with psycopg.connect(target_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version FROM vka_dbversion LIMIT 1")
                result = cur.fetchone()
                return result[0] if result else '0'
    except Exception:
        return '0'


def get_latest_migration_version() -> int:
    """Get the latest migration version from migration files."""
    migration_dir = Path(__file__).parent / "migration"
    if not migration_dir.exists():
        return 0
    
    versions = []
    for file in migration_dir.glob("vkarious_*.sql"):
        match = re.search(r'vkarious_(\d+)\.sql', file.name)
        if match:
            versions.append(int(match.group(1)))
    
    return max(versions) if versions else 0


def execute_migration(migration_file: Path, database_name: str = "vkarious") -> None:
    """Execute a migration file against the specified database."""
    db_dsn = get_database_dsn()
    conn_params = psycopg.conninfo.conninfo_to_dict(db_dsn)
    conn_params['dbname'] = database_name
    target_dsn = psycopg.conninfo.make_conninfo(**conn_params)
    
    with psycopg.connect(target_dsn) as conn:
        with conn.cursor() as cur:
            with open(migration_file, 'r') as f:
                sql = f.read()
            cur.execute(sql)
        conn.commit()


def register_source_database(database_name: str, oid: int) -> None:
    """Register a source database in vka_databases table if not already exists."""
    db_dsn = get_database_dsn()
    conn_params = psycopg.conninfo.conninfo_to_dict(db_dsn)
    conn_params['dbname'] = "vkarious"
    target_dsn = psycopg.conninfo.make_conninfo(**conn_params)
    
    with psycopg.connect(target_dsn) as conn:
        with conn.cursor() as cur:
            # Check if database already exists in vka_databases
            cur.execute("SELECT 1 FROM vka_databases WHERE oid = %s", (oid,))
            if cur.fetchone() is None:
                # Insert the source database record
                cur.execute("""
                    INSERT INTO vka_databases (oid, datname, parent, created_at, type) 
                    VALUES (%s, %s, NULL, %s, 'source')
                """, (oid, database_name, datetime.now()))
        conn.commit()


def register_snapshot_database(snapshot_name: str, snapshot_oid: int, parent_oid: int) -> None:
    """Register a snapshot database in vka_databases table."""
    db_dsn = get_database_dsn()
    conn_params = psycopg.conninfo.conninfo_to_dict(db_dsn)
    conn_params['dbname'] = "vkarious"
    target_dsn = psycopg.conninfo.make_conninfo(**conn_params)
    
    with psycopg.connect(target_dsn) as conn:
        with conn.cursor() as cur:
            # Insert the snapshot database record
            cur.execute("""
                INSERT INTO vka_databases (oid, datname, parent, created_at, type) 
                VALUES (%s, %s, %s, %s, 'snapshot')
            """, (snapshot_oid, snapshot_name, parent_oid, datetime.now()))
        conn.commit()


def get_databases_with_snapshots() -> dict[str, dict]:
    """Get databases from vkarious metadata DB with their snapshots in parent-child relationship."""
    db_dsn = get_database_dsn()
    conn_params = psycopg.conninfo.conninfo_to_dict(db_dsn)
    conn_params['dbname'] = "vkarious"
    target_dsn = psycopg.conninfo.make_conninfo(**conn_params)
    
    with psycopg.connect(target_dsn) as conn:
        with conn.cursor() as cur:
            # Get all databases from vka_databases
            cur.execute("""
                SELECT vd.oid, vd.datname, vd.parent, vd.created_at, vd.type,
                       pg.datname as current_datname
                FROM vka_databases vd
                LEFT JOIN pg_database pg ON vd.oid = pg.oid
                ORDER BY vd.type, vd.created_at
            """)
            
            databases = {}
            snapshots = {}
            
            for row in cur.fetchall():
                oid, datname, parent, created_at, db_type, current_datname = row
                
                # Use current database name if available, fallback to stored name
                display_name = current_datname or datname
                
                db_info = {
                    'oid': oid,
                    'stored_name': datname,
                    'current_name': display_name,
                    'parent': parent,
                    'created_at': created_at,
                    'type': db_type,
                    'snapshots': []
                }
                
                if db_type == 'source':
                    databases[oid] = db_info
                else:  # snapshot
                    snapshots[oid] = db_info
            
            # Organize snapshots under their parent databases
            for snapshot_oid, snapshot_info in snapshots.items():
                parent_oid = snapshot_info['parent']
                if parent_oid in databases:
                    databases[parent_oid]['snapshots'].append(snapshot_info)
            
            return databases


def drop_database(database_name: str) -> None:
    """Drop a database."""
    with connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{database_name}"')


def get_snapshot_record(snapshot_name: str) -> dict | None:
    """Get snapshot record from vka_databases table."""
    db_dsn = get_database_dsn()
    conn_params = psycopg.conninfo.conninfo_to_dict(db_dsn)
    conn_params['dbname'] = "vkarious"
    target_dsn = psycopg.conninfo.make_conninfo(**conn_params)
    
    with psycopg.connect(target_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT oid, datname, parent, created_at, type 
                FROM vka_databases 
                WHERE datname = %s AND type = 'snapshot'
            """, (snapshot_name,))
            result = cur.fetchone()
            if result:
                return {
                    'oid': result[0],
                    'datname': result[1], 
                    'parent': result[2],
                    'created_at': result[3],
                    'type': result[4]
                }
            return None


def delete_database_record(database_name: str) -> None:
    """Delete a database record from vka_databases table."""
    db_dsn = get_database_dsn()
    conn_params = psycopg.conninfo.conninfo_to_dict(db_dsn)
    conn_params['dbname'] = "vkarious"
    target_dsn = psycopg.conninfo.make_conninfo(**conn_params)
    
    with psycopg.connect(target_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM vka_databases WHERE datname = %s", (database_name,))
        conn.commit()


def initialize_database() -> None:
    """Initialize the vkarious database and run migrations."""
    # Check if vkarious database exists, create if not
    if not database_exists("vkarious"):
        create_database("vkarious")
    
    # Check if vka_dbversion table exists
    if not table_exists("vka_dbversion"):
        # Run initial migration
        migration_dir = Path(__file__).parent / "migration"
        initial_migration = migration_dir / "vkarious_1.sql"
        if initial_migration.exists():
            execute_migration(initial_migration)
        return
    
    # Check if we need to run additional migrations
    current_version = int(get_current_version())
    latest_version = get_latest_migration_version()
    
    if current_version < latest_version:
        migration_dir = Path(__file__).parent / "migration"
        for version in range(current_version + 1, latest_version + 1):
            migration_file = migration_dir / f"vkarious_{version}.sql"
            if migration_file.exists():
                execute_migration(migration_file)
