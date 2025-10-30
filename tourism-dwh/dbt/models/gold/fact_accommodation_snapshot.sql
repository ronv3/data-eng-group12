{{ config(materialized='table', schema='gold') }}

WITH s AS (
  SELECT
    registry_code,
    accommodation_name,
    toStartOfQuarter(period_date) AS quarter_start,
    rooms_cnt,
    beds_total,
    beds_high_season,
    beds_low_season,
    caravan_spots,
    tent_spots
  FROM {{ ref('stg_housing_accommodation') }}
  WHERE registry_code IS NOT NULL AND accommodation_name IS NOT NULL
),
da AS (
  SELECT
    accommodation_sk,
    property_bk    AS registry_code,
    name           AS accommodation_name,
    effective_from,
    effective_to
  FROM {{ ref('dim_accommodation') }}
),
dc AS (
  SELECT company_sk, registry_code, effective_from, effective_to
  FROM {{ ref('dim_company') }}
),
dq AS (
  SELECT quarter_sk, quarter_start
  FROM {{ ref('dim_calendar_quarter') }}
),
dg AS (
  SELECT geo_sk, region, county, municipality, island
  FROM {{ ref('dim_geography') }}
),
geo_hint AS (
  -- derive object geo from housing (region/island) and company geo from dim_company if you later extend dim_geography
  SELECT
    h.registry_code,
    anyLast(h.region)      AS region,
    anyLast(h.island)      AS island
  FROM {{ ref('stg_housing_accommodation') }} h
  GROUP BY h.registry_code
)
SELECT
  da.accommodation_sk,
  dc.company_sk,
  dq.quarter_sk,
  dg.geo_sk,

  toInt32OrZero(s.rooms_cnt)        AS rooms_cnt,
  toInt32OrZero(s.beds_total)       AS beds_total,
  toInt32OrZero(s.beds_high_season) AS beds_high_season,
  toInt32OrZero(s.beds_low_season)  AS beds_low_season,
  toInt32OrZero(s.caravan_spots)    AS caravan_spots,
  toInt32OrZero(s.tent_spots)       AS tent_spots
FROM s
JOIN dq
  ON dq.quarter_start = s.quarter_start
LEFT JOIN da
  ON lowerUTF8(s.registry_code)      = lowerUTF8(da.registry_code)
 AND lowerUTF8(s.accommodation_name) = lowerUTF8(da.accommodation_name)
 AND s.quarter_start >= da.effective_from
 AND s.quarter_start <  da.effective_to
LEFT JOIN dc
  ON dc.registry_code = s.registry_code
 AND s.quarter_start >= dc.effective_from
 AND s.quarter_start <  dc.effective_to
LEFT JOIN geo_hint gh
  ON gh.registry_code = s.registry_code
LEFT JOIN {{ ref('dim_geography') }} dg
  ON lowerUTF8(coalesce(gh.region,'')) = lowerUTF8(coalesce(dg.region,''))
 -- county/municipality may be null in housing; left as null match for now
;
