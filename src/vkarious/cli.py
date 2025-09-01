"""Command-line interface for vkarious."""

from __future__ import annotations

import subprocess

import click

from . import __version__


def run_command(command: list[str]) -> None:
    """Run a shell command and echo its output."""
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    click.echo(result.stdout.strip())


@click.group()
def cli() -> None:
    """Manage PostgreSQL database snapshots."""


@cli.command()
@click.argument("database_name")
def snapshot(database_name: str) -> None:
    """Create a snapshot of DATABASE_NAME."""
    run_command(["echo", f"Snapshotting database {database_name}"])


@cli.group()
def snapshots() -> None:
    """Manage snapshots."""


@snapshots.command(name="list")
def list_snapshots() -> None:
    """List available snapshots."""
    click.echo("No snapshots available")


@cli.command()
@click.argument("database_name")
@click.argument("snapshot_id")
def restore(database_name: str, snapshot_id: str) -> None:
    """Restore DATABASE_NAME from SNAPSHOT_ID."""
    run_command(["echo", f"Restoring {database_name} from {snapshot_id}"])


@cli.command()
def version() -> None:
    """Display the vkarious version."""
    click.echo(__version__)
