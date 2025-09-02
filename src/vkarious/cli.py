"""Command-line interface for vkarious."""

from __future__ import annotations

import subprocess

import click

from . import __version__
from .db import (
    copy_database_files,
    create_snapshot_database,
    database_write_lock,
    get_data_directory,
    get_database_oid,
    initialize_database,
    list_databases,
    register_snapshot_database,
    register_source_database,
)


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
    """List available snapshots."""
    try:
        dbs = list_databases()
        snapshots = [db for db in dbs if db['name'].startswith('snapshot_')]
        
        if not snapshots:
            click.echo("No snapshots available")
            return
        
        click.echo(f"{'OID':<10} {'Snapshot Name'}")
        click.echo("-" * 50)
        for snapshot in snapshots:
            click.echo(f"{snapshot['oid']:<10} {snapshot['name']}")
    except Exception as e:
        click.echo(f"Error listing snapshots: {e}", err=True)
        raise click.ClickException(str(e))


@cli.command()
@click.argument("database_name")
@click.argument("snapshot_id")
def restore(database_name: str, snapshot_id: str) -> None:
    """Restore DATABASE_NAME from SNAPSHOT_ID."""
    run_command(["echo", f"Restoring {database_name} from {snapshot_id}"])


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
