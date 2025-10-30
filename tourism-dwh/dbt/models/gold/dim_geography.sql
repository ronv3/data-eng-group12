{{ config(materialized='table', schema='gold') }}

WITH src AS (
  -- Prefer object-level geography from housing; fall back to company-level (tax) where needed
  SELECT DISTINCT
    lowerUTF8(coalesce(h.region, ''))               AS region,
    lowerUTF8(coalesce(c.county, ''))               AS county,
    lowerUTF8(coalesce(c.municipality, ''))         AS municipality,
    lowerUTF8(coalesce(h.island, ''))               AS island
  FROM {{ ref('stg_housing_accommodation') }} h
  LEFT JOIN {{ ref('stg_company_latest') }} c
    ON c.registry_code = h.registry_code
)
SELECT
  xxHash64(region || '|' || county || '|' || municipality || '|' || island) AS geo_sk,
  nullIf(region,      '') AS region,
  nullIf(county,      '') AS county,
  nullIf(municipality,'') AS municipality,
  nullIf(island,      '') AS island
FROM src;
