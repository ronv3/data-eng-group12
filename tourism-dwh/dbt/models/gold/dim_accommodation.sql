{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by=['accommodation_sk','effective_from'],
    settings={'allow_nullable_key': 1}
) }}

WITH src AS (
  SELECT
    -- matches snapshot BK exactly
    lowerUTF8(toString(concat(toString(property_bk), '|', coalesce(name, '')))) AS accommodation_bk,
    lowerUTF8(toString(property_bk))                                            AS property_bk_norm,
    nullIf(replaceRegexpAll(toString(type),     '^[[:space:]]+|[[:space:]]+$', ''), '') AS type,
    nullIf(replaceRegexpAll(toString(category), '^[[:space:]]+|[[:space:]]+$', ''), '') AS category,
    toUInt16(coalesce(stars, 0))                                                AS stars,
    -- dbt snapshot SCD2 fields
    assumeNotNull(toDateTime(dbt_valid_from))                                   AS effective_from,
    assumeNotNull(coalesce(dbt_valid_to, toDateTime('9999-12-31 23:59:59')))    AS effective_to
  FROM {{ ref('accommodation_snapshot') }}
  WHERE property_bk IS NOT NULL
)

SELECT
  xxHash64(accommodation_bk)                                                    AS accommodation_sk,
  accommodation_bk,
  property_bk_norm,
  type,
  category,
  stars,
  effective_from,
  effective_to,
  toUInt8(effective_to >= toDateTime('9999-01-01 00:00:00'))                    AS is_current
FROM src