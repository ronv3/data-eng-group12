-- airflow/include/clickhouse_ddl.sql
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.housing_raw
(
    period_date      Date,
    source_url       String,
    source_file      String,
    raw_json         String,         -- raw row (JSON string)
    record_hash      String,         -- sha256 of normalized JSON
    ingested_at      DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(period_date)
ORDER BY (record_hash);

CREATE TABLE IF NOT EXISTS bronze.tax_raw
(
    period_quarter   Date,           -- first day of quarter (e.g., 2025-10-01)
    which            LowCardinality(String),  -- 'latest' or 'historical'
    source_url       String,
    source_file      String,
    raw_json         String,
    record_hash      String,
    ingested_at      DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(period_quarter)   -- partitions as 202501, 202504, ...
ORDER BY (record_hash);
