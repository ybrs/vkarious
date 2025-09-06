# Change Capture Design

## Overview
vkarious installs change data capture (CDC) on each database that participates in branching. CDC uses a trigger-driven log table to record row-level changes.

## Components
- **vka_cdc table** – stores change events with columns:
  - `id` bigserial primary key
  - `table_name` text
  - `operation` text (INSERT, UPDATE, DELETE)
  - `data` jsonb (row payload)
  - `changed_at` timestamptz default `now()`
- **vka_capture() function** – PL/pgSQL trigger function inserting row changes into `vka_cdc`. It records `OLD` on DELETE and `NEW` for INSERT/UPDATE.
- **Triggers** – for every user table in the `public` schema, a trigger named `vka_<table>_cdc` is created:
  - Fires AFTER INSERT, UPDATE, or DELETE
  - Invokes `vka_capture()`
  - The installer skips the `vka_cdc` table and removes any accidental trigger on it to avoid recursion.

## Installation Flow
1. Determine DSN from `VKA_DATABASE` and connect to the target database.
2. Create `vka_cdc` and `vka_capture()` if missing.
3. Enumerate `pg_tables` in `public` and create missing triggers.
4. Re-run installer to update triggers when new tables appear.

## Branch Interaction
During `vkarious branch`:
1. Register source in metadata and ensure change capture on source.
2. Create branch database using `STRATEGY='FILE_COPY'`.
3. Copy physical files and reset ownership to `postgres:postgres`.
4. Install change capture on the new branch.
5. Insert operations on either database are logged in their respective `vka_cdc` tables.

## Verifying CDC
- Inspect triggers: `SELECT tgname FROM pg_trigger WHERE tgname LIKE 'vka_%';`
- Inspect captured rows: `SELECT * FROM vka_cdc;`
- Reset log: `TRUNCATE vka_cdc;`
