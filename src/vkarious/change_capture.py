"""Change data capture trigger installation for vkarious."""

from __future__ import annotations

import psycopg

from .db import get_database_dsn


class ChangeCaptureInstaller:
    """Install triggers to capture table changes."""

    def ensure_installed(self, database_name: str) -> bool:
        """Ensure change capture is installed on the given database.

        Returns True when any installation actions were performed.
        Returns False when everything was already present.
        """
        dsn = get_database_dsn()
        params = psycopg.conninfo.conninfo_to_dict(dsn)
        params["dbname"] = database_name
        target_dsn = psycopg.conninfo.make_conninfo(**params)

        installed = False

        with psycopg.connect(target_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.vka_cdc')")
                result = cur.fetchone()
                table_exists = result[0] is not None
                if not table_exists:
                    cur.execute(
                        """
                        CREATE TABLE vka_cdc (
                            id BIGSERIAL PRIMARY KEY,
                            table_name TEXT,
                            operation TEXT,
                            data JSONB,
                            changed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE OR REPLACE FUNCTION vka_capture() RETURNS trigger AS $$
                        BEGIN
                            IF TG_OP = 'DELETE' THEN
                                INSERT INTO vka_cdc(table_name, operation, data)
                                    VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD));
                                RETURN OLD;
                            ELSE
                                INSERT INTO vka_cdc(table_name, operation, data)
                                    VALUES (TG_TABLE_NAME, TG_OP, row_to_json(NEW));
                                RETURN NEW;
                            END IF;
                        END;
                        $$ LANGUAGE plpgsql
                        """
                    )
                    installed = True

                cur.execute(
                    """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname = 'public'
                    """
                )
                tables = cur.fetchall()
                index = 0
                while index < len(tables):
                    table_name = tables[index][0]
                    if table_name == "vka_cdc":
                        cur.execute(
                            "DROP TRIGGER IF EXISTS vka_vka_cdc_cdc ON vka_cdc"
                        )
                        index = index + 1
                        continue
                    trigger_name = f"vka_{table_name}_cdc"
                    cur.execute(
                        "SELECT 1 FROM pg_trigger WHERE tgname = %s", (trigger_name,)
                    )
                    trigger_row = cur.fetchone()
                    if trigger_row is None:
                        cur.execute(
                            f"""
                            CREATE TRIGGER {trigger_name}
                            AFTER INSERT OR UPDATE OR DELETE ON {table_name}
                            FOR EACH ROW EXECUTE FUNCTION vka_capture()
                            """
                        )
                        installed = True
                    index = index + 1
            conn.commit()
        return installed
