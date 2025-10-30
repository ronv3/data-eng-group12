{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_GOLD','gold')) }}

WITH f AS (
  SELECT
    lowerUTF8(toString(registry_code)) AS registry_code_norm,
    lowerUTF8(coalesce(county,''))     AS county_norm,
    lowerUTF8(coalesce(municipality,'')) AS municipality_norm,
    toStartOfQuarter(quarter_start)    AS quarter_start,
    turnover_eur, state_taxes_eur, labour_taxes_eur, employees_cnt
  FROM {{ ref('stg_tax_company_quarter') }}
  WHERE registry_code IS NOT NULL
),
dc AS (
  SELECT company_sk, lowerUTF8(toString(registry_code)) AS registry_code_norm
  FROM {{ ref('dim_company') }}
  WHERE is_current = 1
),
dg AS (
  SELECT geo_sk,
         lowerUTF8(coalesce(county,''))      AS county_norm,
         lowerUTF8(coalesce(municipality,'')) AS municipality_norm
  FROM {{ ref('dim_geography') }}
),
dq AS (
  SELECT quarter_sk, quarter_start
  FROM {{ ref('dim_calendar_quarter') }}
)
SELECT
  dc.company_sk,
  dq.quarter_sk,
  dg.geo_sk,
  ifNull(f.turnover_eur,     0.0) AS turnover_eur,
  ifNull(f.state_taxes_eur,  0.0) AS state_taxes_eur,
  ifNull(f.labour_taxes_eur, 0.0) AS labour_taxes_eur,
  ifNull(f.employees_cnt,       0) AS employees_cnt
FROM f
JOIN dq ON dq.quarter_start = f.quarter_start
LEFT JOIN dc ON f.registry_code_norm = dc.registry_code_norm
LEFT JOIN dg ON f.county_norm = dg.county_norm
            AND f.municipality_norm = dg.municipality_norm
