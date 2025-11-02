{{ config(materialized='table', schema='gold') }}

WITH f AS (
  SELECT property_bk AS registry_code, accommodation_name, feature_name
  FROM {{ ref('stg_housing_features') }}
),
d AS (
  SELECT
    accommodation_sk,
    property_bk,
    name
  FROM {{ ref('dim_accommodation') }}
  WHERE is_current
),
feat AS (
  SELECT feature_sk, lowerUTF8(feature_name) AS fname
  FROM {{ ref('dim_feature') }}
)
SELECT
  d.accommodation_sk,
  feat.feature_sk
FROM f
JOIN d
  ON lowerUTF8(f.registry_code)      = lowerUTF8(d.property_bk)
 AND lowerUTF8(f.accommodation_name) = lowerUTF8(d.name)
JOIN feat
  ON lowerUTF8(f.feature_name)       = feat.fname
GROUP BY d.accommodation_sk, feat.feature_sk
