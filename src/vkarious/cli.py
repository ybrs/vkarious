"""Command-line interface for vkarious."""

from __future__ import annotations

import subprocess

import click

from . import __version__
from .db import (
    copy_database_files,
    create_branch_database,
    create_snapshot_database,
    database_exists,
    database_write_lock,
    delete_database_record,
    drop_database,
    get_data_directory,
    get_database_oid,
    get_databases_with_snapshots,
    get_snapshot_record,
    initialize_database,
    list_databases,
    log_branch_operation,
    register_branch_database,
    register_snapshot_database,
    register_source_database,
    restore_database_from_snapshot,
)
from .change_capture import ChangeCaptureInstaller


def run_command(command: list[str]) -> None:
    """Run a shell command and echo its output."""
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    click.echo(result.stdout.strip())


@click.group()
def cli() -> None:
    """Manage PostgreSQL database snapshots."""
    initialize_database()


@cli.command()
@click.argument("database_name")
@click.argument("branch_name")
def branch(database_name: str, branch_name: str) -> None:
    """Create a branch of DATABASE_NAME with the given BRANCH_NAME."""
    try:
        click.echo(f"Creating branch '{branch_name}' of database '{database_name}'...")
        
        # Get source database OID
        source_oid = get_database_oid(database_name)
        click.echo(f"Source database OID: {source_oid}")
        
        # Register source database in vka_databases table
        register_source_database(database_name, source_oid)
        click.echo(f"Registered source database '{database_name}' in vka_databases")
        
        # Ensure change-capture is installed on the source
        installer = ChangeCaptureInstaller()
        if installer.ensure_installed(database_name):
            click.echo("Installed vkarious change-capture on source database")
        else:
            click.echo("vkarious change-capture already present on source database")

        # Get PostgreSQL data directory
        data_directory = get_data_directory()
        click.echo(f"PostgreSQL data directory: {data_directory}")
        
        # Create new branch database and get its OID
        branch_database_name, target_oid = create_branch_database(database_name, branch_name)
        click.echo(f"Created branch database '{branch_database_name}' with OID: {target_oid}")
        
        # Lock the source database to prevent writes during copy
        with database_write_lock(database_name):
            click.echo(f"Acquired write lock on database '{database_name}'")
            
            # Copy database files
            copy_database_files(data_directory, source_oid, target_oid)
            click.echo("Database files copied successfully")

        # Ensure change-capture is present on the new branch database as well
        if installer.ensure_installed(branch_database_name):
            click.echo("Installed vkarious change-capture on branch database")
        else:
            click.echo("vkarious change-capture already present on branch database")
        
        # Register branch database in vka_databases table
        register_branch_database(branch_database_name, target_oid, source_oid)
        click.echo(f"Registered branch '{branch_database_name}' in vka_databases with parent OID {source_oid}")
        
        # Log branch creation operation
        log_branch_operation(source_oid, target_oid, branch_database_name)
        click.echo(f"Logged branch creation operation to vka_log")
        
        click.echo(f"Branch completed successfully: {branch_database_name}")
        
    except Exception as e:
        click.echo(f"Error creating branch: {e}", err=True)
        raise click.ClickException(str(e))


@cli.command()
@click.argument("database_name")
def snapshot(database_name: str) -> None:
    """Create a snapshot of DATABASE_NAME."""
    try:
        click.echo(f"Creating snapshot of database '{database_name}'...")
        
        # Get source database OID
        source_oid = get_database_oid(database_name)
        click.echo(f"Source database OID: {source_oid}")
        
        # Register source database in vka_databases table
        register_source_database(database_name, source_oid)
        click.echo(f"Registered source database '{database_name}' in vka_databases")
        
        # Get PostgreSQL data directory
        data_directory = get_data_directory()
        click.echo(f"PostgreSQL data directory: {data_directory}")
        
        # Create new snapshot database and get its OID
        snapshot_name, target_oid = create_snapshot_database(database_name)
        click.echo(f"Created snapshot database '{snapshot_name}' with OID: {target_oid}")
        
        # Lock the source database to prevent writes during copy
        with database_write_lock(database_name):
            click.echo(f"Acquired write lock on database '{database_name}'")
            
            # Copy database files
            copy_database_files(data_directory, source_oid, target_oid)
            click.echo("Database files copied successfully")
        
        # Register snapshot database in vka_databases table
        register_snapshot_database(snapshot_name, target_oid, source_oid)
        click.echo(f"Registered snapshot '{snapshot_name}' in vka_databases with parent OID {source_oid}")
        
        click.echo(f"Snapshot completed successfully: {snapshot_name}")
        
    except Exception as e:
        click.echo(f"Error creating snapshot: {e}", err=True)
        raise click.ClickException(str(e))


@cli.group()
def snapshots() -> None:
    """Manage snapshots."""


@snapshots.command(name="list")
def list_snapshots() -> None:
    """List available snapshots organized by database."""
    try:
        databases = get_databases_with_snapshots()
        
        if not databases:
            click.echo("No databases with snapshots found")
            return
        
        for db_oid, db_info in databases.items():
            # Show database name with indication if renamed
            db_name = db_info['current_name']
            if db_info['current_name'] != db_info['stored_name']:
                db_name = f"{db_info['current_name']} (was: {db_info['stored_name']})"
            
            click.echo(f"\nDatabase: {db_name} (OID: {db_oid})")
            
            if not db_info['snapshots']:
                click.echo("  No snapshots available")
            else:
                click.echo(f"  {'OID':<10} {'Snapshot Name':<30} {'Created'}")
                click.echo("  " + "-" * 60)
                for snapshot in db_info['snapshots']:
                    created = snapshot['created_at'].strftime('%Y-%m-%d %H:%M:%S') if snapshot['created_at'] else 'Unknown'
                    click.echo(f"  {snapshot['oid']:<10} {snapshot['current_name']:<30} {created}")
    except Exception as e:
        click.echo(f"Error listing snapshots: {e}", err=True)
        raise click.ClickException(str(e))


@snapshots.command(name="delete")
@click.argument("snapshot_name")
def delete_snapshot(snapshot_name: str) -> None:
    """Delete a snapshot by name."""
    try:
        click.echo(f"Deleting snapshot '{snapshot_name}'...")
        
        # Check if snapshot exists in vka_databases with type 'snapshot'
        snapshot_record = get_snapshot_record(snapshot_name)
        if snapshot_record is None:
            click.echo(f"Error: Snapshot '{snapshot_name}' does not exist", err=True)
            raise click.ClickException(f"Snapshot '{snapshot_name}' does not exist")
        
        # Check if the snapshot database exists
        if not database_exists(snapshot_name):
            # Database doesn't exist, remove from vka_databases table and exit with warning
            delete_database_record(snapshot_name)
            click.echo(f"Warning: Database '{snapshot_name}' does not exist but was tracked in metadata. Removed from tracking.")
            return
        
        # Drop the database
        drop_database(snapshot_name)
        click.echo(f"Dropped database '{snapshot_name}'")
        
        # Delete the record from vka_databases
        delete_database_record(snapshot_name)
        click.echo(f"Removed record for '{snapshot_name}' from vka_databases")
        
        click.echo(f"Snapshot '{snapshot_name}' deleted successfully")
        
    except Exception as e:
        click.echo(f"Error deleting snapshot: {e}", err=True)
        raise click.ClickException(str(e))

@snapshots.command(name="restore")
@click.argument("database_name")
@click.argument("snapshot_name")
def restore_snapshot_cmd(database_name: str, snapshot_name: str) -> None:
    """Restore a database from a snapshot's physical files.

    Usage: snapshots restore <database_name> <snapshot_name>
    """
    try:
        click.echo(
            f"Restoring database '{database_name}' from snapshot '{snapshot_name}'..."
        )

        details = restore_database_from_snapshot(database_name, snapshot_name)

        click.echo(
            f"Moved original data directory to: {details['backup_path']}"
        )
        click.echo(
            f"Restored database OID {details['restored_oid']} from snapshot OID {details['snapshot_oid']}"
        )
        click.echo(
            f"Connected successfully. Public tables found: {details['tables_count']}"
        )
        click.echo("Restore completed successfully")

    except Exception as e:
        try:
            base = get_data_directory()
            base_msg = f" Check '{base}/base' for a directory prefixed with 'vka_delete_' containing the original files."
        except Exception:
            base_msg = " Original data files were moved aside with a 'vka_delete_' prefix."
        click.echo(f"Error restoring snapshot: {e}.{base_msg}", err=True)
        raise click.ClickException(str(e))


@cli.group()
def databases() -> None:
    """Manage databases."""


@databases.command(name="list")
def list_databases_cmd() -> None:
    """List databases with their OIDs and names."""
    try:
        dbs = list_databases()
        if not dbs:
            click.echo("No databases found")
            return
        
        click.echo(f"{'OID':<10} {'Database Name'}")
        click.echo("-" * 30)
        for db in dbs:
            click.echo(f"{db['oid']:<10} {db['name']}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        raise click.ClickException(str(e))


@cli.command()
def version() -> None:
    """Display the vkarious version."""
    click.echo(__version__)
