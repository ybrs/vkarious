# vkarious

vkarious is a tool to get snapshots of PostgreSQL databases.

**Warning:** This project is heavily work in progress.

## Configuration

Set the `VKA_DATABASE` environment variable to your PostgreSQL connection string:

```bash
export VKA_DATABASE="postgresql://username:password@localhost:5432/database_name"
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

List snapshots:
```bash
vkarious snapshots list
```

Restore from a snapshot:
```bash
vkarious restore database_name snapshot_id
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
