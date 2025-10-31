{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_GOLD','gold')) }}

-- Staging rows (silver) with normalized registry code available
WITH s AS (
  SELECT
    -- keep raw for debugging if you want
    toString(property_bk)            AS property_bk_raw,
    lowerUTF8(toString(property_bk)) AS property_bk_norm,

    -- NEW: join key from silver (digits-only)
    -- If your silver already outputs digits-only 'registry_code', this is redundant but harmless
    lowerUTF8(replaceRegexpAll(toString(registry_code), '[^0-9]', '')) AS registry_code_lc_digits,

    name                             AS accommodation_name_raw,
    lowerUTF8(name)                  AS accommodation_name_norm,
    toStartOfQuarter(period_date)    AS quarter_start,
    rooms_cnt, beds_total, beds_high_season, beds_low_season, caravan_spots, tent_spots
  FROM {{ ref('stg_housing_accommodation') }}
  WHERE name IS NOT NULL
),

-- Current accommodation dimension (resolve accommodation_sk)
da AS (
  SELECT
    accommodation_sk,
    lowerUTF8(toString(property_bk)) AS property_bk_norm,
    lowerUTF8(name)                  AS accommodation_name_norm
  FROM {{ ref('dim_accommodation') }}
  WHERE is_current = 1
),

-- Current company dimension (resolve company_sk) — normalize to digits-only, too
dc AS (
  SELECT
    company_sk,
    lowerUTF8(replaceRegexpAll(toString(registry_code), '[^0-9]', '')) AS registry_code_lc_digits
  FROM {{ ref('dim_company') }}
  WHERE is_current = 1
),

dq AS (
  SELECT quarter_sk, quarter_start
  FROM {{ ref('dim_calendar_quarter') }}
),

-- Hint geo from staging, then map to a stable geo_sk 1:1 per (region,island)
geo_hint AS (
  SELECT
    lowerUTF8(toString(property_bk))         AS property_bk_norm,
    lowerUTF8(coalesce(anyLast(region), '')) AS region_norm,
    lowerUTF8(coalesce(anyLast(island), '')) AS island_norm
  FROM {{ ref('stg_housing_accommodation') }}
  GROUP BY property_bk
),
dg AS (
  SELECT
    anyHeavy(geo_sk)                   AS geo_sk,
    lowerUTF8(coalesce(region,''))     AS region_norm,
    lowerUTF8(coalesce(island,''))     AS island_norm
  FROM {{ ref('dim_geography') }}
  GROUP BY region_norm, island_norm
)

SELECT
  da.accommodation_sk,
  dc.company_sk,     -- will be NULL if no registry code or no matching company
  dq.quarter_sk,
  dg.geo_sk,
  s.rooms_cnt, s.beds_total, s.beds_high_season, s.beds_low_season, s.caravan_spots, s.tent_spots
FROM s
JOIN dq  ON dq.quarter_start = s.quarter_start
LEFT JOIN da
  ON s.property_bk_norm = da.property_bk_norm
 AND s.accommodation_name_norm = da.accommodation_name_norm
LEFT JOIN dc
  ON dc.registry_code_lc_digits = s.registry_code_lc_digits
LEFT JOIN geo_hint gh
  ON gh.property_bk_norm = s.property_bk_norm
LEFT JOIN dg
  ON dg.region_norm = gh.region_norm
 AND dg.island_norm = gh.island_norm