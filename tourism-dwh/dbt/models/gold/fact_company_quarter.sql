{{ config(materialized='table', schema='gold') }}

WITH f AS (
  SELECT
    registry_code,
    county,
    municipality,
    year,
    quarter,
    quarter_start,
    turnover_eur,
    state_taxes_eur,
    labour_taxes_eur,
    employees_cnt
  FROM {{ ref('stg_tax_company_quarter') }}
  WHERE registry_code IS NOT NULL
),
dc AS (  -- correct SCD row for the quarter
  SELECT
    company_sk,
    registry_code,
    effective_from,
    effective_to
  FROM {{ ref('dim_company') }}
),
dg AS (
  SELECT geo_sk, region, county, municipality, island
  FROM {{ ref('dim_geography') }}
),
dq AS (
  SELECT quarter_sk, year, quarter, quarter_start
  FROM {{ ref('dim_calendar_quarter') }}
)
SELECT
  dc.company_sk,
  dq.quarter_sk,
  dg.geo_sk,

  -- measures
  toFloat64OrZero(turnover_eur)      AS turnover_eur,
  toFloat64OrZero(state_taxes_eur)   AS state_taxes_eur,
  toFloat64OrZero(labour_taxes_eur)  AS labour_taxes_eur,
  toInt32OrZero(employees_cnt)       AS employees_cnt
FROM f
JOIN dq
  ON dq.quarter_start = f.quarter_start
LEFT JOIN dc
  ON dc.registry_code = f.registry_code
 AND f.quarter_start >= dc.effective_from
 AND f.quarter_start <  dc.effective_to
LEFT JOIN dg
  ON lowerUTF8(coalesce(f.county,''))       = lowerUTF8(coalesce(dg.county,''))
 AND lowerUTF8(coalesce(f.municipality,'')) = lowerUTF8(coalesce(dg.municipality,''))
;
