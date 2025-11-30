-- Base bronze database and tables
CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.housing_raw
(
    period_date   Date,
    source_url    String,
    source_file   String,
    raw_json      String,
    record_hash   String,
    ingested_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(period_date)
ORDER BY record_hash;

CREATE TABLE IF NOT EXISTS bronze.tax_raw
(
    period_quarter Date,
    which          LowCardinality(String),
    source_url     String,
    source_file    String,
    raw_json       String,
    record_hash    String,
    ingested_at    DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(period_quarter)
ORDER BY record_hash;

-- Read-only Iceberg view of bronze.tax_raw
CREATE DATABASE IF NOT EXISTS iceberg_bronze;

CREATE TABLE IF NOT EXISTS iceberg_bronze.tax_raw
(
    period_quarter Date,
    which          String,
    source_url     String,
    source_file    String,
    raw_json       String,
    record_hash    String,
    ingested_at    DateTime64(3)
)
ENGINE = Iceberg(
    '{{ICEBERG_TAX_ROOT_URL}}',
    '{{S3_ACCESS_KEY_ID}}',
    '{{S3_SECRET_ACCESS_KEY}}',
    'Parquet'
);
