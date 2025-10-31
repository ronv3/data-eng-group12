{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_GOLD','gold')) }}

{% set use_latest = var('use_latest_company', false) %}

-- Source facts from Tax (one row per company-quarter)
WITH f AS (
  SELECT
    lowerUTF8(toString(registry_code))             AS registry_code_lc,
    lowerUTF8(coalesce(county, ''))                AS county_lc,
    lowerUTF8(coalesce(municipality, ''))          AS municipality_lc,
    quarter_start,                                 -- Date
    turnover_eur, state_taxes_eur, labour_taxes_eur, employees_cnt
  FROM {{ ref('stg_tax_company_quarter') }}
  WHERE registry_code IS NOT NULL
),

-- De-duplicate geography to 1:1 per (county, municipality)
dg AS (
  SELECT
    anyHeavy(geo_sk) AS geo_sk,
    county_lc, municipality_lc
  FROM (
    SELECT
      geo_sk,
      lowerUTF8(coalesce(county, ''))       AS county_lc,
      lowerUTF8(coalesce(municipality, '')) AS municipality_lc
    FROM {{ ref('dim_geography') }}
  )
  GROUP BY county_lc, municipality_lc
),

-- Quarter dimension (with end date for SCD windowing)
dq AS (
  SELECT quarter_sk, quarter_start, quarter_end
  FROM {{ ref('dim_calendar_quarter') }}
)

{% if use_latest %}
-- LATEST-ONLY mapping: 1 company_sk per registry_code (ignore SCD windows)
, dc_map AS (
  SELECT
    lowerUTF8(toString(registry_code)) AS registry_code_lc,
    argMax(company_sk, effective_from) AS company_sk
  FROM {{ ref('dim_company') }}
  GROUP BY lowerUTF8(toString(registry_code))
)

SELECT
  m.company_sk,
  q.quarter_sk,
  g.geo_sk,
  coalesce(f.turnover_eur,     0.) AS turnover_eur,
  coalesce(f.state_taxes_eur,  0.) AS state_taxes_eur,
  coalesce(f.labour_taxes_eur, 0.) AS labour_taxes_eur,
  coalesce(f.employees_cnt,      0) AS employees_cnt
FROM f
JOIN dq AS q
  ON q.quarter_start = f.quarter_start
LEFT JOIN dc_map AS m
  ON m.registry_code_lc = f.registry_code_lc
LEFT JOIN dg AS g
  ON g.county_lc = f.county_lc
 AND g.municipality_lc = f.municipality_lc

{% else %}
-- SCD-aware mapping: keep rows only where the quarter intersects the SCD interval
, dc AS (
  SELECT
    company_sk,
    lowerUTF8(toString(registry_code)) AS registry_code_lc,
    effective_from, effective_to
  FROM {{ ref('dim_company') }}
)

SELECT
  dc.company_sk,
  q.quarter_sk,
  g.geo_sk,
  coalesce(f.turnover_eur,     0.) AS turnover_eur,
  coalesce(f.state_taxes_eur,  0.) AS state_taxes_eur,
  coalesce(f.labour_taxes_eur, 0.) AS labour_taxes_eur,
  coalesce(f.employees_cnt,      0) AS employees_cnt
FROM f
JOIN dq AS q
  ON q.quarter_start = f.quarter_start
LEFT JOIN dc
  ON dc.registry_code_lc = f.registry_code_lc
LEFT JOIN dg AS g
  ON g.county_lc = f.county_lc
 AND g.municipality_lc = f.municipality_lc
WHERE
  dc.company_sk IS NULL
  OR (
    toDateTime(q.quarter_end)   >= dc.effective_from
    AND toDateTime(q.quarter_start) <  dc.effective_to
  )
{% endif %}