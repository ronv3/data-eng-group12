CREATE DATABASE IF NOT EXISTS default_bronze;

CREATE TABLE IF NOT EXISTS default_bronze.housing_raw
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
