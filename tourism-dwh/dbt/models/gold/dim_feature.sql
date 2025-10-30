{{ config(materialized='table', schema='gold') }}

WITH base AS (
  SELECT DISTINCT feature_name
  FROM {{ ref('stg_housing_features') }}
  WHERE feature_name IS NOT NULL AND feature_name != ''
)
SELECT
  xxHash64(lowerUTF8(feature_name)) AS feature_sk,
  feature_name
FROM base
