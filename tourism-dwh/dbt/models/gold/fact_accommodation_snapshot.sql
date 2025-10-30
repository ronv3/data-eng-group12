{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_GOLD','gold')) }}

WITH s AS (
  SELECT
    toString(property_bk)            AS registry_code_raw,
    lowerUTF8(toString(property_bk)) AS registry_code_norm,
    name                             AS accommodation_name_raw,
    lowerUTF8(name)                  AS accommodation_name_norm,
    toStartOfQuarter(period_date)    AS quarter_start,
    rooms_cnt, beds_total, beds_high_season, beds_low_season, caravan_spots, tent_spots
  FROM {{ ref('stg_housing_accommodation') }}
  WHERE property_bk IS NOT NULL AND name IS NOT NULL
),
da AS (
  SELECT
    accommodation_sk,
    lowerUTF8(toString(property_bk)) AS registry_code_norm,
    lowerUTF8(name)                  AS accommodation_name_norm
  FROM {{ ref('dim_accommodation') }}
  WHERE is_current = 1
),
dc AS (
  SELECT
    company_sk,
    lowerUTF8(toString(registry_code)) AS registry_code_norm
  FROM {{ ref('dim_company') }}
  WHERE is_current = 1
),
dq AS (
  SELECT quarter_sk, quarter_start
  FROM {{ ref('dim_calendar_quarter') }}
),
geo_hint AS (
  SELECT
    lowerUTF8(toString(property_bk))         AS registry_code_norm,
    lowerUTF8(coalesce(anyLast(region), '')) AS region_norm,
    lowerUTF8(coalesce(anyLast(island), '')) AS island_norm
  FROM {{ ref('stg_housing_accommodation') }}
  GROUP BY property_bk
),
dg AS (
  SELECT
    geo_sk,
    lowerUTF8(coalesce(region,'')) AS region_norm,
    lowerUTF8(coalesce(island,'')) AS island_norm
  FROM {{ ref('dim_geography') }}
)
SELECT
  da.accommodation_sk,
  dc.company_sk,
  dq.quarter_sk,
  dg.geo_sk,
  s.rooms_cnt, s.beds_total, s.beds_high_season, s.beds_low_season, s.caravan_spots, s.tent_spots
FROM s
JOIN dq  ON dq.quarter_start = s.quarter_start
LEFT JOIN da ON s.registry_code_norm = da.registry_code_norm
           AND s.accommodation_name_norm = da.accommodation_name_norm
LEFT JOIN dc ON s.registry_code_norm = dc.registry_code_norm
LEFT JOIN geo_hint gh ON gh.registry_code_norm = s.registry_code_norm
LEFT JOIN dg ON dg.region_norm = gh.region_norm
            AND dg.island_norm = gh.island_norm
