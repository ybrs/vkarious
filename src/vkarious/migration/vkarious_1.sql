CREATE TABLE vka_dbversion (
    version VARCHAR(255) DEFAULT '0'
);

INSERT INTO vka_dbversion (version) VALUES ('0');

CREATE TABLE vka_databases (
    oid INTEGER,
    datname VARCHAR(255),
    parent INTEGER,
    created_at TIMESTAMP,
    type VARCHAR(254)
);

UPDATE vka_dbversion SET version = '1';