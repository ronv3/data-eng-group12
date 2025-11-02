{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by=['company_sk','effective_from'],
    settings={'allow_nullable_key': 1}
) }}

WITH src AS (
  SELECT
    lowerUTF8(replaceRegexpAll(toString(registry_code), '\\D', ''))             AS company_id,  -- normalized BK
    toString(name)         AS name,
    toString(activity)     AS activity,
    toString(website)      AS website,
    toString(email)        AS email,
    toString(phone)        AS phone,
    toString(address)      AS address,
    toString(municipality) AS municipality,
    toString(county)       AS county,
    assumeNotNull(toDateTime(dbt_valid_from))                                   AS effective_from,
    assumeNotNull(coalesce(dbt_valid_to, toDateTime('9999-12-31 23:59:59')))    AS effective_to
  FROM {{ ref('company_snapshot') }}
  WHERE registry_code IS NOT NULL
)

SELECT
  xxHash64(company_id) AS company_sk,
  company_id,
  name,
  activity,
  website,
  email,
  phone,
  address,
  municipality,
  county,
  effective_from,
  effective_to,
  toUInt8(effective_to >= toDateTime('9999-01-01 00:00:00')) AS is_current
FROM src