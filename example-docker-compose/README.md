# Example Docker Compose (macOS)

This folder contains a minimal setup to run PostgreSQL 17 for local development and a companion Ubuntu 22 tool image with `psql`, `node`, and global npm packages.

## Requirements
- Docker Desktop for Mac

## Quick Start
```bash
cd example-docker-compose
# Build the devtools image
docker compose build
# Start PostgreSQL and devtools
docker compose up -d
# Check status
docker compose ps
# Open a shell in the devtools container
docker compose exec devtools bash
# From inside devtools, connect to Postgres
psql "postgresql://postgres:postgres@postgres:5432/postgres"
```

## Connection Strings
- From host:
  - DSN: `postgresql://postgres:postgres@localhost:5444/postgres`
  - psql: `psql -h localhost -p 5444 -U postgres postgres`
- From devtools container:
  - DSN: `postgresql://postgres:postgres@postgres:5432/postgres`

## Files and Ports
- Data directory: `./pgdata` is bind-mounted to `/var/lib/postgresql/data`
- Published port: host `5444` -> container `5432`

## Common Issues
- No configuration file provided:
  - Run from this folder: `cd example-docker-compose && docker compose up -d`
  - Or specify the file: `docker compose -f example-docker-compose/docker-compose.yml up -d`
- Permissions on `pgdata`:
  - Let Docker create it on first run, or fix with: `sudo chown -R 999:999 ./pgdata`
- Port conflict (5444 already in use):
  - Edit `docker-compose.yml` and change the `ports:` mapping, e.g. `5544:5432`

## Stop and Clean Up
```bash
# Stop containers, keep data
docker compose down
# Stop containers and remove data (irreversible)
docker compose down -v
```

## Notes for macOS (APFS)
- The data folder is a bind mount; performance may be better using `:delegated` (already set).
- Data persists on the host under `pgdata/` between restarts.
