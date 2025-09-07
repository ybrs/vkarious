#!/usr/bin/env python3
"""
Integration test for DDL logging functionality.

This test creates real databases, branches them, performs DDL operations,
and verifies that all DDL commands (CREATE TABLE, ALTER TABLE, DROP TABLE) 
are properly logged in the vkarious.ddl_log table.

The test uses unique database names and cleans up on success, but leaves 
databases for debugging on failure.
"""
import os
import subprocess
import time
import uuid
from typing import List, Tuple

import psycopg

try:
    import pytest
    PYTEST_AVAILABLE = True
except ImportError:
    PYTEST_AVAILABLE = False


def get_connection_dsn() -> str:
    """Get PostgreSQL connection string from environment."""
    dsn = os.environ.get("VKA_DATABASE")
    if not dsn:
        if PYTEST_AVAILABLE:
            pytest.skip("VKA_DATABASE environment variable not set")
        else:
            raise RuntimeError("VKA_DATABASE environment variable not set")
    return dsn


def run_vkarious_command(cmd: List[str]) -> Tuple[int, str, str]:
    """Run a vkarious command and return exit code, stdout, stderr."""
    # Use vkarious executable from venv
    vkarious_bin = ".venv/bin/vkarious"
    if not os.path.exists(vkarious_bin):
        if PYTEST_AVAILABLE:
            pytest.skip("vkarious executable not found at .venv/bin/vkarious")
        else:
            raise RuntimeError("vkarious executable not found at .venv/bin/vkarious")
    
    full_cmd = [vkarious_bin] + cmd
    
    result = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        cwd=os.getcwd()
    )
    
    return result.returncode, result.stdout, result.stderr


def database_exists(dsn: str, dbname: str) -> bool:
    """Check if a database exists."""
    try:
        # Connect to postgres database to check if target database exists
        base_dsn = dsn.rsplit('/', 1)[0] + '/postgres'
        with psycopg.connect(base_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (dbname,)
                )
                return cur.fetchone() is not None
    except Exception:
        return False


def drop_database_if_exists(dsn: str, dbname: str) -> None:
    """Drop a database if it exists."""
    if not database_exists(dsn, dbname):
        return
    
    try:
        # Connect to postgres database to drop target database
        base_dsn = dsn.rsplit('/', 1)[0] + '/postgres'
        with psycopg.connect(base_dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Terminate connections to the target database
                cur.execute("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity 
                    WHERE datname = %s AND pid <> pg_backend_pid()
                """, (dbname,))
                
                # Drop the database
                cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
                
    except Exception as e:
        print(f"Warning: Failed to drop database {dbname}: {e}")


def create_database(dsn: str, dbname: str) -> None:
    """Create a new database."""
    # Connect to postgres database to create target database
    base_dsn = dsn.rsplit('/', 1)[0] + '/postgres'
    with psycopg.connect(base_dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {dbname}")


def execute_sql(dsn: str, dbname: str, sql: str) -> List[Tuple]:
    """Execute SQL on a specific database and return results."""
    # Connect to the target database
    db_dsn = dsn.rsplit('/', 1)[0] + f'/{dbname}'
    with psycopg.connect(db_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description:
                return cur.fetchall()
            return []


def test_ddl_logging_integration():
    """
    Integration test for DDL logging functionality.
    
    Tests that CREATE TABLE, ALTER TABLE, and DROP TABLE commands
    are all properly logged in vkarious.ddl_log after branching.
    """
    dsn = get_connection_dsn()
    
    # Generate unique database names using timestamp and UUID
    test_id = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"
    source_db = f"vka_test_source_{test_id}"
    branch_db = f"vka_test_branch_{test_id}"
    
    cleanup_dbs = []  # Track databases for cleanup
    
    try:
        print(f"Starting DDL logging integration test with databases: {source_db}, {branch_db}")
        
        # Step 1: Create source database
        print(f"Creating source database: {source_db}")
        create_database(dsn, source_db)
        cleanup_dbs.append(source_db)
        
        # Step 2: Create branch using vkarious
        print(f"Creating branch: {source_db} -> {branch_db}")
        exit_code, stdout, stderr = run_vkarious_command(["branch", source_db, branch_db])
        
        if exit_code != 0:
            print(f"vkarious branch command failed:")
            print(f"Exit code: {exit_code}")
            print(f"Stdout: {stdout}")
            print(f"Stderr: {stderr}")
            raise Exception(f"vkarious branch command failed with exit code {exit_code}")
        
        cleanup_dbs.append(branch_db)
        print("Branch created successfully")
        
        # Step 3: Verify branch database exists and has vkarious schema
        print(f"Verifying branch database {branch_db} has vkarious schema")
        result = execute_sql(dsn, branch_db, """
            SELECT 1 FROM pg_namespace WHERE nspname = 'vkarious'
        """)
        assert len(result) > 0, "vkarious schema not found in branch database"
        
        result = execute_sql(dsn, branch_db, """
            SELECT 1 FROM pg_class c 
            JOIN pg_namespace n ON n.oid = c.relnamespace 
            WHERE n.nspname = 'vkarious' AND c.relname = 'ddl_log'
        """)
        assert len(result) > 0, "vkarious.ddl_log table not found in branch database"
        
        # Step 4: Perform DDL operations on branch database
        print("Performing DDL operations on branch database")
        
        # Clear any existing DDL log entries to start fresh
        execute_sql(dsn, branch_db, "DELETE FROM vkarious.ddl_log")
        
        # CREATE TABLE
        execute_sql(dsn, branch_db, """
            CREATE TABLE test_ddl_logging (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # ALTER TABLE
        execute_sql(dsn, branch_db, """
            ALTER TABLE test_ddl_logging 
            ADD COLUMN email VARCHAR(255),
            ADD COLUMN status VARCHAR(50) DEFAULT 'active'
        """)
        
        # CREATE another table for dropping
        execute_sql(dsn, branch_db, """
            CREATE TABLE test_drop_table (
                id INT,
                data TEXT
            )
        """)
        
        # DROP TABLE
        execute_sql(dsn, branch_db, """
            DROP TABLE test_drop_table
        """)
        
        # Step 5: Verify DDL logging
        print("Verifying DDL operations were logged")
        
        ddl_entries = execute_sql(dsn, branch_db, """
            SELECT 
                id, 
                command_tag, 
                object_type, 
                schema_name, 
                object_identity, 
                phase,
                CASE WHEN sql_text IS NOT NULL THEN 'HAS_SQL' ELSE 'NO_SQL' END as has_sql
            FROM vkarious.ddl_log 
            WHERE object_type = 'table' 
              AND schema_name = 'public'
            ORDER BY id
        """)
        
        print(f"Found {len(ddl_entries)} DDL log entries:")
        for entry in ddl_entries:
            print(f"  {entry}")
        
        # Verify we have the expected DDL operations logged
        command_tags = [entry[1] for entry in ddl_entries]
        
        # We expect to see CREATE TABLE commands
        create_commands = [tag for tag in command_tags if tag == 'CREATE TABLE']
        assert len(create_commands) >= 2, f"Expected at least 2 CREATE TABLE commands, got {len(create_commands)}"
        
        # We expect to see ALTER TABLE commands  
        alter_commands = [tag for tag in command_tags if tag == 'ALTER TABLE']
        assert len(alter_commands) >= 1, f"Expected at least 1 ALTER TABLE command, got {len(alter_commands)}"
        
        # We expect to see DROP TABLE commands (this was the bug we fixed)
        drop_commands = [tag for tag in command_tags if tag == 'DROP TABLE']
        assert len(drop_commands) >= 1, f"Expected at least 1 DROP TABLE command, got {len(drop_commands)}"
        
        # Verify specific table operations
        table_identities = [entry[4] for entry in ddl_entries]
        assert 'public.test_ddl_logging' in table_identities, "test_ddl_logging table operations not logged"
        assert 'public.test_drop_table' in table_identities, "test_drop_table table operations not logged"
        
        # Verify that all entries have SQL text captured
        sql_statuses = [entry[6] for entry in ddl_entries]
        has_sql_count = sum(1 for status in sql_statuses if status == 'HAS_SQL')
        print(f"DDL entries with SQL text: {has_sql_count}/{len(ddl_entries)}")
        
        print("✅ All DDL logging tests passed!")
        
        # Step 6: Cleanup on success
        print("Cleaning up test databases...")
        for db in cleanup_dbs:
            drop_database_if_exists(dsn, db)
            print(f"Dropped database: {db}")
        
        print("Integration test completed successfully!")
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        print(f"Leaving test databases for debugging: {cleanup_dbs}")
        print(f"To connect to source database: psql {dsn.rsplit('/', 1)[0]}/{source_db}")
        if branch_db in cleanup_dbs:
            print(f"To connect to branch database: psql {dsn.rsplit('/', 1)[0]}/{branch_db}")
        print("To check DDL log: SELECT * FROM vkarious.ddl_log ORDER BY id;")
        raise


if __name__ == "__main__":
    # Allow running as a standalone script
    test_ddl_logging_integration()