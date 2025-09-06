We implemented this todo below, but getting errors. Please fix it


TODO:
=====

Below is a self-contained SQL setup using schema `vkarious`. It captures DML with type-aware payloads and logs DDL with timestamps, and auto-installs row triggers for new tables that have a primary key.

When we branch a database we need to inject these triggers both to the source and cloned database. 

Keep these triggers inside the codebase as sql files. When we get a branch command, check if these exist on the source db, if not add them. 

Also inject them into the branched, new database. 


### 1) Core schema and tables

```sql
CREATE SCHEMA IF NOT EXISTS vkarious;

CREATE TABLE IF NOT EXISTS vkarious.change_log (
  id bigserial PRIMARY KEY,
  rel regclass NOT NULL,
  op char(1) NOT NULL,
  key jsonb NOT NULL,
  cols jsonb,
  tx xid8 NOT NULL DEFAULT txid_current(),
  ts timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS vkarious.ddl_log (
  id bigserial PRIMARY KEY,
  ts timestamptz NOT NULL DEFAULT clock_timestamp(),
  username text NOT NULL DEFAULT current_user,
  dbname name NOT NULL DEFAULT current_database(),
  tx xid8 NOT NULL DEFAULT txid_current(),
  command_tag text NOT NULL,
  object_type text,
  schema_name text,
  object_identity text,
  phase text NOT NULL,
  sql_text text,
  pre_def text,
  post_def text
);
```

### 2) Helpers for PK discovery and key serialization

```sql
CREATE OR REPLACE FUNCTION vkarious.pk_names(rel regclass) RETURNS text[]
LANGUAGE sql STABLE AS $$
SELECT array_agg(a.attname ORDER BY a.attnum)
FROM pg_index i
JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey)
WHERE i.indrelid=$1 AND i.indisprimary
$$;

CREATE OR REPLACE FUNCTION vkarious.pk_json(rel regclass, rec anyelement) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  j jsonb := '{}'::jsonb;
  att smallint;
  name name;
  v jsonb;
BEGIN
  FOR att IN SELECT unnest(i.indkey) FROM pg_index i WHERE i.indrelid=rel AND i.indisprimary LOOP
    SELECT a.attname INTO name FROM pg_attribute a WHERE a.attrelid=rel AND a.attnum=att AND NOT a.attisdropped;
    EXECUTE format('SELECT to_jsonb(($1).%I)', name) INTO v USING rec;
    j := j || jsonb_build_object(name, v);
  END LOOP;
  IF j = '{}'::jsonb THEN RAISE EXCEPTION 'no primary key on %', rel; END IF;
  RETURN j;
END$$;
```

### 3) DML capture trigger (type-aware)

```sql
CREATE OR REPLACE FUNCTION vkarious.capture() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,vkarious AS $$
DECLARE
  keys jsonb;
  delta jsonb := '{}'::jsonb;
  col record;
  pkcols text[] := vkarious.pk_names(TG_RELID::regclass);
  v_new text;
  v_old text;
  entry jsonb;
BEGIN
  IF TG_OP='INSERT' THEN
    keys := vkarious.pk_json(TG_RELID::regclass, NEW);
    FOR col IN
      SELECT a.attname AS name, a.atttypid AS toid, a.atttypmod AS m, a.attnum
      FROM pg_attribute a
      WHERE a.attrelid=TG_RELID AND a.attnum>0 AND NOT a.attisdropped
    LOOP
      EXECUTE format('SELECT ($1).%I::text', col.name) INTO v_new USING NEW;
      entry := jsonb_build_object('toid', col.toid, 'm', col.m, 'v', v_new);
      delta := delta || jsonb_build_object(col.name, entry);
    END LOOP;
    INSERT INTO vkarious.change_log(rel,op,key,cols) VALUES (TG_RELID::regclass,'I',keys,delta);
    RETURN NEW;
  ELSIF TG_OP='UPDATE' THEN
    keys := vkarious.pk_json(TG_RELID::regclass, NEW);
    FOR col IN
      SELECT a.attname AS name, a.atttypid AS toid, a.atttypmod AS m, a.attnum
      FROM pg_attribute a
      WHERE a.attrelid=TG_RELID AND a.attnum>0 AND NOT a.attisdropped
    LOOP
      IF col.name = ANY(pkcols) THEN
        EXECUTE format('SELECT ($1).%I::text', col.name) INTO v_new USING NEW;
        EXECUTE format('SELECT ($1).%I::text', col.name) INTO v_old USING OLD;
        IF v_new IS DISTINCT FROM v_old THEN
          entry := jsonb_build_object('toid', col.toid, 'm', col.m, 'v', v_new);
          delta := delta || jsonb_build_object(col.name, entry);
        END IF;
      ELSE
        EXECUTE format('SELECT ($1).%I::text', col.name) INTO v_new USING NEW;
        EXECUTE format('SELECT ($1).%I::text', col.name) INTO v_old USING OLD;
        IF v_new IS DISTINCT FROM v_old THEN
          entry := jsonb_build_object('toid', col.toid, 'm', col.m, 'v', v_new);
          delta := delta || jsonb_build_object(col.name, entry);
        END IF;
      END IF;
    END LOOP;
    IF delta <> '{}'::jsonb THEN
      INSERT INTO vkarious.change_log(rel,op,key,cols) VALUES (TG_RELID::regclass,'U',keys,delta);
    END IF;
    RETURN NEW;
  ELSIF TG_OP='DELETE' THEN
    keys := vkarious.pk_json(TG_RELID::regclass, OLD);
    INSERT INTO vkarious.change_log(rel,op,key,cols) VALUES (TG_RELID::regclass,'D',keys,NULL);
    RETURN OLD;
  END IF;
END$$;
```

### 4) Apply a single log row to the base table

```sql
CREATE OR REPLACE FUNCTION vkarious.apply_row(log_id bigint) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  r record;
  k text;
  setlist text := '';
  coltype text;
  q text;
  first boolean := true;
BEGIN
  SELECT * INTO r FROM vkarious.change_log WHERE id=log_id;
  IF r.op='I' THEN
    q := 'INSERT INTO '||r.rel||'('||
         (SELECT string_agg(quote_ident(k), ',') FROM jsonb_object_keys(r.cols) t(k))||
         ') VALUES ('||
         (
           SELECT string_agg(
                    CASE
                      WHEN (r.cols->k->>'v') IS NULL THEN 'NULL'
                      ELSE format('(%L)::%s', r.cols->k->>'v', format_type((r.cols->k->>'toid')::oid, (r.cols->k->>'m')::int))
                    END, ',')
           FROM jsonb_object_keys(r.cols) t(k)
         )||')';
    EXECUTE q;
  ELSIF r.op='U' THEN
    IF r.cols IS NULL THEN RETURN; END IF;
    FOR k IN SELECT key FROM jsonb_object_keys(r.cols) LOOP
      coltype := format_type((r.cols->k->>'toid')::oid, (r.cols->k->>'m')::int);
      IF NOT first THEN setlist := setlist||', '; END IF;
      setlist := setlist||quote_ident(k)||' = '||
                 CASE WHEN (r.cols->k->>'v') IS NULL THEN 'NULL'
                      ELSE format('(%L)::%s', r.cols->k->>'v', coltype) END;
      first := false;
    END LOOP;
    q := 'UPDATE '||r.rel||' SET '||setlist||' WHERE ('||
         (SELECT string_agg(quote_ident(k), ',') FROM jsonb_object_keys(r.key) t(k))||
         ') = ('||
         (SELECT string_agg(
                   CASE WHEN r.key->>k IS NULL THEN 'NULL'
                        ELSE format('%L', r.key->>k) END, ',')
          FROM jsonb_object_keys(r.key) t(k))||
         ')';
    EXECUTE q;
  ELSIF r.op='D' THEN
    q := 'DELETE FROM '||r.rel||' WHERE ('||
         (SELECT string_agg(quote_ident(k), ',') FROM jsonb_object_keys(r.key) t(k))||
         ') = ('||
         (SELECT string_agg(
                   CASE WHEN r.key->>k IS NULL THEN 'NULL'
                        ELSE format('%L', r.key->>k) END, ',')
          FROM jsonb_object_keys(r.key) t(k))||
         ')';
    EXECUTE q;
  END IF;
END$$;
```

### 5) Install row triggers on existing user tables that have a PK

```sql
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT c.oid::regclass AS rel
    FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relkind='r'
      AND n.nspname NOT IN ('pg_catalog','information_schema','vkarious')
      AND EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indisprimary)
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS vkarious_row ON %s', r.rel);
    EXECUTE format('CREATE TRIGGER vkarious_row AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION vkarious.capture()', r.rel);
  END LOOP;
END$$;
```

### 6) Auto-install triggers for new tables with a PK

```sql
CREATE OR REPLACE FUNCTION vkarious.install_trigger_for(rel regclass) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid=rel AND i.indisprimary) THEN
    EXECUTE format('DROP TRIGGER IF EXISTS vkarious_row ON %s', rel);
    EXECUTE format('CREATE TRIGGER vkarious_row AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION vkarious.capture()', rel);
  END IF;
END$$;

CREATE OR REPLACE FUNCTION vkarious.on_ddl_end_install() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    IF r.schema_name NOT IN ('pg_catalog','information_schema','vkarious') THEN
      IF r.object_type IN ('table','partitioned table','table partition') THEN
        PERFORM vkarious.install_trigger_for((quote_ident(r.schema_name)||'.'||quote_ident(r.object_name))::regclass);
      END IF;
    END IF;
  END LOOP;
END$$;

DROP EVENT TRIGGER IF EXISTS vkarious_ddl_end_install;
CREATE EVENT TRIGGER vkarious_ddl_end_install ON ddl_command_end EXECUTE FUNCTION vkarious.on_ddl_end_install();
```

### 7) DDL auditing with timestamps

```sql
CREATE OR REPLACE FUNCTION vkarious.ddl_start() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    INSERT INTO vkarious.ddl_log(command_tag,object_type,schema_name,object_identity,phase,pre_def)
    VALUES (
      r.command_tag,r.object_type,r.schema_name,r.object_identity,'start',
      CASE r.object_type
        WHEN 'view' THEN pg_get_viewdef(r.objid,true)
        WHEN 'materialized view' THEN pg_get_viewdef(r.objid,true)
        WHEN 'function' THEN pg_get_functiondef(r.objid)
        WHEN 'index' THEN pg_get_indexdef(r.objid)
        ELSE NULL
      END
    );
  END LOOP;
END$$;

CREATE OR REPLACE FUNCTION vkarious.ddl_end() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    INSERT INTO vkarious.ddl_log(command_tag,object_type,schema_name,object_identity,phase,post_def)
    VALUES (
      r.command_tag,r.object_type,r.schema_name,r.object_identity,'end',
      CASE r.object_type
        WHEN 'view' THEN pg_get_viewdef(r.objid,true)
        WHEN 'materialized view' THEN pg_get_viewdef(r.objid,true)
        WHEN 'function' THEN pg_get_functiondef(r.objid)
        WHEN 'index' THEN pg_get_indexdef(r.objid)
        ELSE NULL
      END
    );
  END LOOP;
  FOR r IN SELECT command, object_type FROM pg_event_trigger_get_creation_commands() LOOP
    INSERT INTO vkarious.ddl_log(command_tag,object_type,phase,sql_text)
    VALUES ('CREATE', r.object_type, 'end', r.command);
  END LOOP;
END$$;

CREATE OR REPLACE FUNCTION vkarious.on_table_rewrite() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_table_rewrite() LOOP
    INSERT INTO vkarious.ddl_log(command_tag,object_type,object_identity,phase)
    VALUES ('TABLE REWRITE','table', r.relid::regclass::text, 'end');
  END LOOP;
END$$;

DROP EVENT TRIGGER IF EXISTS vkarious_ddl_start;
CREATE EVENT TRIGGER vkarious_ddl_start ON ddl_command_start EXECUTE FUNCTION vkarious.ddl_start();

DROP EVENT TRIGGER IF EXISTS vkarious_ddl_end;
CREATE EVENT TRIGGER vkarious_ddl_end ON ddl_command_end EXECUTE FUNCTION vkarious.ddl_end();

DROP EVENT TRIGGER IF EXISTS vkarious_table_rewrite;
CREATE EVENT TRIGGER vkarious_table_rewrite ON table_rewrite EXECUTE FUNCTION vkarious.on_table_rewrite();
```

Also write these as unit tests. We need to check if this works correctly.


### 8) Basic smoke test

```sql
CREATE TABLE IF NOT EXISTS public.vk_demo(id int primary key, geom geometry(Point,4326), txt text, n numeric(10,2));
INSERT INTO public.vk_demo VALUES (1, ST_GeomFromText('POINT(1 2)',4326), 'a', 10.50);
UPDATE public.vk_demo SET txt='b', n=11.75 WHERE id=1;
DELETE FROM public.vk_demo WHERE id=1;
SELECT rel::text, op, key, cols, tx, ts FROM vkarious.change_log ORDER BY id;
SELECT ts, command_tag, object_type, schema_name, object_identity, phase, (sql_text IS NOT NULL) AS has_sql FROM vkarious.ddl_log ORDER BY id DESC LIMIT 20;
```

### 9) Applying rows

```sql
SELECT vkarious.apply_row(id) FROM vkarious.change_log ORDER BY id;
```

