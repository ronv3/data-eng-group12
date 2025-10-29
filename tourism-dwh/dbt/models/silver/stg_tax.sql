{{ config(materialized='table', schema='silver') }}

SELECT
  period_quarter,
  which,
  ingested_at,
  /* Examples - replace with actual keys from tax CSV */
  JSONExtractString(raw_json, 'Region')                     AS region,
  toDateOrNull(JSONExtractString(raw_json, 'Date'))         AS date_in_row,
  toFloat64OrNull(JSONExtractString(raw_json, 'TaxAmount')) AS tax_amount,
  toInt32OrNull(JSONExtractString(raw_json, 'Taxpayers'))   AS taxpayer_count
FROM {{ source('bronze','tax_raw') }};
