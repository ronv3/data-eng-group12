{{ config(materialized='table', schema='gold') }}

WITH q AS (
  SELECT DISTINCT
    toYear(quarter_start)   AS year,
    toQuarter(quarter_start) AS quarter,
    toStartOfQuarter(quarter_start) as quarter_start
  FROM {{ ref('stg_tax_company_quarter') }}
)
SELECT
  xxHash64(toString(year) || '-Q' || toString(quarter))             AS quarter_sk,
  year,
  quarter,
  quarter_start,
  addDays(addMonths(quarter_start, 3), -1)                          AS quarter_end
FROM q;
