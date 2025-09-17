We have this extension to hash/digest a table.

we build the extension with these

```
PG17_PGCONFIG="$(brew --prefix postgresql@17)/bin/pg_config"
cargo pgrx init --pg17 "$PG17_PGCONFIG"

cargo pgrx package --pg-config "$PG17_PGCONFIG"

```

and installed with this 

```
# IMPORTANT: build explicitly for PG17 to avoid ABI crashes
cargo pgrx install --release --pg-config "$PG17_PGCONFIG" --no-default-features --features pg17
```

and when we run it with 

```
psql coinleverprod --user aybarsb
```


```
coinleverprod=# \timing
Timing is on.
coinleverprod=# SELECT vkar_hash_table('public.runtime_commandrunhistory'::regclass, 10000);
 27626b9e17bdeee99e6005f670aa5a7a5d3cb5d2a957476466c6d671ab2776a1

Time: 29284.050 ms (00:29.284)
```

Task
- I need you to make this faster. for example we are doing this 

```

```

You've completed this task. 

now we get this issue 

```
coinleverprod=# SELECT vkar_hash_table('public.runtime_commandrunhistory'::regclass, 10000);
ERROR:  FETCH is not allowed in a non-volatile function
CONTEXT:  SQL statement "fetch forward 10000 from vkar_cur"
```

we ran these 

Build and install (PG17)

- Verify pg_config:
    - PG17_PGCONFIG="$(brew --prefix postgresql@17)/bin/pg_config"
    - "$PG17_PGCONFIG" --version  (should print 17.x)
- Initialize pgrx for PG17 (idempotent):
    - cargo pgrx init --pg17 "$PG17_PGCONFIG"
- rebuild:
    - cargo pgrx package --pg-config "$PG17_PGCONFIG"
- Install for PG17 only:
    - cargo pgrx install --release --pg-config "$PG17_PGCONFIG" --no-default-features --features pg17

Verify in psql (server 17)


- Recreate extension:
    - DROP EXTENSION IF EXISTS pg_hashdb; CREATE EXTENSION pg_hashdb;
- Ensure you’ll see messages:
    - Optional: SET client_min_messages = WARNING; (default already shows WARNING)
- Run the function:
    - SELECT vkar_hash_table('public.runtime_commandrunhistory'::regclass, 10000);

Check server logs

- Discover logging targets:
    - SHOW log_destination;
    - SHOW logging_collector;
    - SHOW log_directory;
    - SHOW log_filename;
    - SELECT current_setting('data_directory');
- On Homebrew with logging_collector on, logs are typically under:
    - $(brew --prefix)/var/log/postgresql@17/ or the cluster’s log_directory.
- You should see warnings if cursor declare/fetch/close or OID resolution fails:
    - WARNING: pg_hashdb: failed to declare cursor (rc=...)
    - WARNING: pg_hashdb: FETCH failed (rc=...)
    - WARNING: pg_hashdb: CLOSE CURSOR failed (rc=...)
    - WARNING: pg_hashdb: failed to resolve relation name (rc=...)



here are the logs

```
2025-09-17 12:05:54.452 CEST [34207] LOG:  database system is ready to accept connections
2025-09-17 12:51:41.388 CEST [79694] ERROR:  FETCH is not allowed in a non-volatile function
2025-09-17 12:51:41.388 CEST [79694] CONTEXT:  SQL statement "fetch forward 10000 from vkar_cur"
2025-09-17 12:51:41.388 CEST [79694] STATEMENT:  SELECT vkar_hash_table('public.runtime_commandrunhistory'::regclass, 10000);
```

fix the issue. you can change the function in any way you see fit.

you said you fixed the volatile issue but we are still seeing this

```
2025-09-17 12:57:30.673 CEST [80291] ERROR:  FETCH is not allowed in a non-volatile function
2025-09-17 12:57:30.673 CEST [80291] CONTEXT:  SQL statement "fetch forward 10000 from vkar_cur"
2025-09-17 12:57:30.673 CEST [80291] STATEMENT:  SELECT vkar_hash_table('public.runtime_commandrunhistory'::regclass, 10000);
```

