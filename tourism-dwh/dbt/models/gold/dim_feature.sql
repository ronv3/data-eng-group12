{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by=['feature_sk']
) }}

WITH src AS (
  SELECT
    lowerUTF8(toString(feature_name)) AS feature_name_norm
  FROM {{ ref('stg_housing_features') }}
  WHERE feature_name IS NOT NULL
)

SELECT
  xxHash64(feature_name_norm) AS feature_sk,
  feature_name_norm          AS name
FROM src
GROUP BY feature_name_norm