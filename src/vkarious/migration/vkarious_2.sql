ALTER TABLE vka_databases ADD COLUMN status VARCHAR(255);

CREATE TABLE vka_log (
    id SERIAL PRIMARY KEY,
    old_oid INTEGER,
    new_oid INTEGER,
    datname VARCHAR(255),
    operation VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status VARCHAR(255),
    error_description TEXT
);

UPDATE vka_dbversion SET version = '2';