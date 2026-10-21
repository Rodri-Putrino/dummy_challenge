CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.ingestion_batches (
    batch_seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id text NOT NULL UNIQUE,
    source_uri text NOT NULL,
    file_sha256 text NOT NULL,
    archive_uri text NOT NULL,
    ingested_at timestamptz NOT NULL,
    row_count bigint NOT NULL CHECK (row_count > 0)
);

CREATE TABLE IF NOT EXISTS bronze.transactions_raw (
    ingest_date date NOT NULL,
    raw_row_id text NOT NULL,
    batch_seq bigint NOT NULL REFERENCES audit.ingestion_batches(batch_seq),
    source_line bigint NOT NULL,
    ingested_at timestamptz NOT NULL,
    transaction_id text,
    customer_id text,
    transaction_date text,
    amount text,
    status text,
    country_code text,
    raw_payload jsonb NOT NULL,
    ingestion_error text,
    PRIMARY KEY (ingest_date, raw_row_id)
) PARTITION BY RANGE (ingest_date);
CREATE INDEX IF NOT EXISTS transactions_raw_batch_idx
    ON bronze.transactions_raw (batch_seq);

CREATE TABLE IF NOT EXISTS audit.pipeline_state (
    pipeline_name text PRIMARY KEY,
    last_successful_batch_seq bigint NOT NULL DEFAULT 0
);
INSERT INTO audit.pipeline_state (pipeline_name) VALUES ('daily_sales')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id text PRIMARY KEY,
    started_at timestamptz NOT NULL DEFAULT current_timestamp,
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    batch_seq bigint REFERENCES audit.ingestion_batches(batch_seq),
    lower_seq bigint,
    upper_seq bigint,
    bronze_rows bigint,
    silver_rows bigint,
    quarantine_rows bigint,
    error text
);
