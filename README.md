# vkarious

vkarious is a tool to create snapshots and branches of PostgreSQL databases.

Important: This project is an active work-in-progress. Expect rapid changes, occasional instability, and breaking changes as features evolve. To get notified about updates, use the Watch button on the repository (choose "All Activity"). Manage your watch settings for this repo at: https://github.com/ybrs/vkarious/subscription

## Configuration

Set the `VKA_DATABASE` environment variable to your PostgreSQL connection string:

```bash
export VKA_DATABASE="postgresql://username:password@localhost:5432/database_name"
```

Optionally, set `VKA_PG_DATA_PATH` to override the detected PostgreSQL data directory. This is useful when PostgreSQL runs in a container but vkarious runs on the host and needs a host-visible path for copying database files (e.g., COW file copies):

```bash
# Example: host path where the container's PGDATA is mounted
export VKA_PG_DATA_PATH="/Users/me/docker-volumes/postgres-data"
```

## Usage

List all databases:
```bash
vkarious databases list
```

Create a snapshot:
```bash
vkarious snapshot database_name
```

Create a branch with a custom name:
```bash
vkarious branch database_name branch_name
```

When `VKA_PG_DATA_PATH` is set, vkarious uses that directory for physical file operations instead of querying `SHOW data_directory` from PostgreSQL.

List snapshots:
```bash
vkarious snapshots list
```

Restore from a snapshot:
```bash
vkarious snapshots restore database_name snapshot_name
```

Delete a snapshot:
```bash
vkarious snapshots delete snapshot_name
```

Check version:
```bash
vkarious version
```

# Example
```
export VKA_DATABASE="postgresql://@localhost:5432/postgres"
(vkarious) $ uv run vkarious databases list
OID        Database Name
------------------------------
14042      postgres
4          template0
1          template1
65786      test
```



## Development

Install the project in editable mode and run the CLI using
[uv](https://docs.astral.sh/uv/):

```bash
uv venv
uv pip install -e .
uv run vkarious --help
```

Run the test suite with:

```bash
uv run pytest
```
