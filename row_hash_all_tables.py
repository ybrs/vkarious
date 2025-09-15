import os, sys, time, psycopg, subprocess, shlex
from psycopg import sql

dsn = os.environ["VKA_DATABASE"]

def list_user_tables_with_stats(con):
    with con.cursor() as cur:
        cur.execute("""
            select n.nspname, c.relname, c.oid,
                   greatest(c.reltuples,0)::bigint as est_rows,
                   pg_total_relation_size(c.oid) as total_bytes
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where c.relkind = 'r'
              and n.nspname not in ('pg_catalog','information_schema')
            order by n.nspname, c.relname
        """)
        return cur.fetchall()

def list_columns(con, schema, table):
    with con.cursor() as cur:
        cur.execute("""
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
            order by ordinal_position
        """, (schema, table))
        return [r[0] for r in cur.fetchall()]

def list_pk_columns(con, schema, table):
    with con.cursor() as cur:
        cur.execute("""
            select a.attname
            from pg_index i
            join pg_class c on c.oid = i.indrelid
            join pg_namespace n on n.oid = c.relnamespace
            join pg_attribute a on a.attrelid = c.oid and a.attnum = any(i.indkey)
            where i.indisprimary
              and n.nspname = %s and c.relname = %s
            order by array_position(i.indkey, a.attnum)
        """, (schema, table))
        return [r[0] for r in cur.fetchall()]

def pretty_bytes(b):
    g = 1024**3
    m = 1024**2
    if b >= g:
        return f"{b/g:.2f} GB"
    if b >= m:
        return f"{b/m:.2f} MB"
    return f"{b/1024:.2f} KB"

def digest_table(con, schema, table, use_blake3=True):
    if use_blake3:
        import blake3
        h = blake3.blake3()
    else:
        cmd = shlex.split("openssl dgst -sha256 -binary")
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    cols = list_columns(con, schema, table)
    pk = list_pk_columns(con, schema, table)
    if not cols:
        return "", 0
    select_list = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    if pk:
        order_by = sql.SQL(", ").join(sql.Identifier(c) for c in pk)
    else:
        order_by = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    q = sql.SQL("COPY (SELECT {select_list} FROM {tbl} ORDER BY {order_by}) TO STDOUT (FORMAT binary)").format(
        select_list=select_list,
        tbl=sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table)),
        order_by=order_by,
    )
    streamed = 0
    with con.cursor() as cur:
        with cur.copy(q) as cp:
            for chunk in cp:
                streamed += len(chunk)
                if use_blake3:
                    h.update(chunk)
                else:
                    p.stdin.write(chunk)
    if use_blake3:
        return h.hexdigest(), streamed
    p.stdin.close()
    return p.stdout.read().hex(), streamed

def main():
    use_blake3 = True
    if len(sys.argv) > 1 and sys.argv[1].lower() == "--sha256":
        use_blake3 = False
        sys.argv.pop(1)
    table_arg = sys.argv[1] if len(sys.argv) > 1 else None
    with psycopg.connect(dsn, autocommit=True) as con:
        if table_arg:
            if "." in table_arg:
                schema, table = table_arg.split(".", 1)
            else:
                schema = "public"; table = table_arg
            tables = [(schema, table, None, None, None)]
        else:
            tables = list_user_tables_with_stats(con)
        if table_arg:
            total_rows = 0
            total_bytes = 0
        else:
            total_rows = sum(t[3] for t in tables)
            total_bytes = sum(t[4] for t in tables)
        start_all = time.perf_counter()
        processed_bytes = 0
        for t in tables:
            schema, table = t[0], t[1]
            if table_arg:
                with con.cursor() as cur:
                    cur.execute("""
                        select greatest(c.reltuples,0)::bigint, pg_total_relation_size(c.oid)
                        from pg_class c join pg_namespace n on n.oid=c.relnamespace
                        where n.nspname=%s and c.relname=%s
                    """, (schema, table))
                    r = cur.fetchone() or (0, 0)
                    est_rows, total_b = int(r[0]), int(r[1])
            else:
                est_rows, total_b = int(t[3]), int(t[4])
            t0 = time.perf_counter()
            digest, streamed = digest_table(con, schema, table, use_blake3=use_blake3)
            dt = time.perf_counter() - t0
            processed_bytes += total_b
            spent = time.perf_counter() - start_all
            rate = (streamed/dt) if dt > 0 else 0
            if total_bytes and spent > 0:
                left = (total_bytes - processed_bytes) / ((processed_bytes)/spent) if processed_bytes else 0
                bytes_pct = (total_b/total_bytes)*100 if total_bytes else 0
                rows_pct = (est_rows/total_rows)*100 if total_rows else 0
            else:
                left = 0
                bytes_pct = 0
                rows_pct = 0
            print(
                f"{schema}.{table} {digest} "
                f"size {pretty_bytes(total_b)} ({bytes_pct:.2f}% of db) "
                f"rows~{est_rows} ({rows_pct:.2f}% of db) "
                f"took {dt:.2f}s "
                f"rate {pretty_bytes(rate)}/s "
                f"spent {spent:.2f}s left~{left:.2f}s"
            )

if __name__ == "__main__":
    main()
