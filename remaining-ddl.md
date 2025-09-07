# Remaining DDL Gaps

This document lists DDL operations that the current capture system does not record with full fidelity.

## Alter Table lacks full context
The audit stores the raw `ALTER TABLE` statement but omits the resulting table definition. Column renames or constraint changes therefore lack before/after details.

*Example*
```sql
ALTER TABLE users ADD CONSTRAINT users_email_key UNIQUE (email);
```
Only the above statement is logged; the new table structure is not.

## Drops for non-table objects
`sql_drop` triggers are limited to table-like objects. Dropping views, functions, or sequences does not create a `ddl_log` entry.

*Example*
```sql
DROP VIEW recent_orders;
```
No row is recorded.

## Unsupported object types
Operations such as `CREATE SCHEMA`, `ALTER SEQUENCE`, or `CREATE TYPE` are ignored by the DDL audit.

*Example*
```sql
ALTER SEQUENCE order_id_seq RESTART WITH 1000;
```
This command is not captured.

## Dynamic DDL via EXECUTE
Event triggers do not see dynamically generated DDL executed through `EXECUTE`, leaving the log empty for those statements.

*Example*
```sql
EXECUTE format('ALTER TABLE %I ADD COLUMN %I int', tab, col);
```

