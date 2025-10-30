{{ config(materialized='table', schema='gold') }}

WITH src AS (
  SELECT
    registry_code,
    company_name,
    activity,
    company_website,
    company_email,
    company_phone,
    company_address,
    municipality,
    county,
    dbt_valid_from,
    dbt_valid_to
  FROM {{ ref('company_snapshot') }}
)
SELECT
  -- SCD2 surrogate key per version
  xxHash64(lowerUTF8(registry_code) || '|' || toString(dbt_valid_from)) AS company_sk,

  registry_code,
  company_name        AS name,
  activity,
  company_website     AS website,
  company_email       AS email,
  company_phone       AS phone,
  company_address     AS address,
  municipality,
  county,

  toDateTime(dbt_valid_from)                                          AS effective_from,
  toDateTime(coalesce(dbt_valid_to, toDateTime('9999-12-31 23:59:59'))) AS effective_to,
  dbt_valid_to IS NULL                                                AS is_current
FROM src
WHERE registry_code IS NOT NULL;
