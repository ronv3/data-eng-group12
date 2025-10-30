{{ config(materialized='table', schema='gold') }}

WITH qsrc AS (
  SELECT DISTINCT toStartOfQuarter(period_date) AS quarter_start
  FROM {{ ref('stg_housing_accommodation') }}
  WHERE period_date IS NOT NULL

  UNION ALL

  SELECT DISTINCT quarter_start
  FROM {{ ref('stg_tax_company_quarter') }}
)
SELECT
  toUInt32(toYear(quarter_start) * 10 + toQuarter(quarter_start)) AS quarter_sk,
  toYear(quarter_start)                                           AS year,
  toQuarter(quarter_start)                                        AS quarter,
  quarter_start                                                   AS quarter_start,
  toDate(addMonths(quarter_start, 3) - 1)                         AS quarter_end
FROM qsrc
GROUP BY quarter_start
