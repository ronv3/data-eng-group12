{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_GOLD','gold')) }}              AND dg.island_norm = gh.island_norm, not registry code)
WITH s AS (
  SELECT
    toString(property_bk)            AS property_bk_raw,
    lowerUTF8(toString(property_bk)) AS property_bk_norm,
    name                             AS accommodation_name_raw,
    lowerUTF8(name)                  AS accommodation_name_norm,
    toStartOfQuarter(period_date)    AS quarter_start,
    rooms_cnt, beds_total, beds_high_season, beds_low_season, caravan_spots, tent_spots
  FROM {{ ref('stg_housing_accommodation') }}
  WHERE property_bk IS NOT NULL AND name IS NOT NULL
),

-- Map property -> company registry_code (seed)
pcm AS (
  SELECT
    lowerUTF8(toString(property_bk)) AS property_bk_norm,
    lowerUTF8(toString(registry_code)) AS registry_code_norm
  FROM {{ ref('property_company_map') }}
),

-- Current accommodation dimension (to resolve accommodation_sk)
da AS (
  SELECT
    accommodation_sk,
    lowerUTF8(toString(property_bk)) AS property_bk_norm,
    lowerUTF8(name)                  AS accommodation_name_norm
  FROM {{ ref('dim_accommodation') }}
  WHERE is_current = 1
),

-- Current company dimension (to resolve company_sk)
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

-- Hint geo from staging
geo_hint AS (
  SELECT
    lowerUTF8(toString(property_bk))         AS property_bk_norm,
    lowerUTF8(coalesce(anyLast(region), '')) AS region_norm,
    lowerUTF8(coalesce(anyLast(island), '')) AS island_norm
  FROM {{ ref('stg_housing_accommodation') }}
  GROUP BY property_bk
),

-- Deduplicate geography to 1 row per (region,island) to avoid fanout
dg AS (
  SELECT
    anyHeavy(geo_sk)                          AS geo_sk,
    lowerUTF8(coalesce(region,''))            AS region_norm,
    lowerUTF8(coalesce(island,''))            AS island_norm
  FROM {{ ref('dim_geography') }}
  GROUP BY region_norm, island_norm
)

SELECT
  da.accommodation_sk,
  dc.company_sk,         -- will be non-null when a mapping exists
  dq.quarter_sk,
  dg.geo_sk,
  s.rooms_cnt, s.beds_total, s.beds_high_season, s.beds_low_season, s.caravan_spots, s.tent_spots
FROM s
JOIN dq  ON dq.quarter_start = s.quarter_start
LEFT JOIN da ON s.property_bk_norm = da.property_bk_norm
           AND s.accommodation_name_norm = da.accommodation_name_norm
LEFT JOIN pcm ON pcm.property_bk_norm = s.property_bk_norm
LEFT JOIN dc  ON dc.registry_code_norm = pcm.registry_code_norm
LEFT JOIN geo_hint gh ON gh.property_bk_norm = s.property_bk_norm
LEFT JOIN dg ON dg.region_norm = gh.region_norm
            AND dg.island_norm = gh.island_norm