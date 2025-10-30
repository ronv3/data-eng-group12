{{ config(materialized='table', schema='gold') }}

WITH src AS (
  SELECT
    accommodation_bk,
    property_bk,
    name,
    category,
    type,
    stars,
    booking_link,
    facebook,
    instagram,
    tiktok,
    seasonal_flag,
    dbt_valid_from,
    dbt_valid_to
  FROM {{ ref('accommodation_snapshot') }}
)
SELECT
  xxHash64(lowerUTF8(accommodation_bk) || '|' || toString(dbt_valid_from)) AS accommodation_sk,
  property_bk,
  name,
  category,
  type,
  stars,
  booking_link,
  concat(
    '{',
      '"facebook":"',  coalesce(facebook,  ''), '",',
      '"instagram":"', coalesce(instagram, ''), '",',
      '"tiktok":"',    coalesce(tiktok,    ''), '"',
    '}'
  ) AS socials,
  seasonal_flag,
  toDateTime(dbt_valid_from) AS effective_from,
  toDateTime(coalesce(dbt_valid_to, toDateTime('9999-12-31 23:59:59'))) AS effective_to,
  dbt_valid_to IS NULL AS is_current
FROM src
