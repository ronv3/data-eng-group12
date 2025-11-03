{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_SILVER','silver')) }}

WITH tax_latest AS (
  SELECT
    registry_code,
    anyLast(company_name) AS name,
    anyLast(activity)     AS activity,
    anyLast(municipality) AS municipality,
    anyLast(county)       AS county
  FROM {{ ref('stg_tax_company_quarter') }}
  GROUP BY registry_code
),
contact AS (
  SELECT
    property_bk              AS registry_code,
    anyLast(company_website) AS website,
    anyLast(company_email)   AS email,
    anyLast(company_phone)   AS phone,
    anyLast(company_address) AS address
  FROM {{ ref('stg_housing_accommodation') }}
  WHERE property_bk IS NOT NULL
  GROUP BY property_bk
)
SELECT
  t.registry_code,
  t.name,
  t.activity,
  coalesce(c.website, '') AS website,
  coalesce(c.email,   '') AS email,
  coalesce(c.phone,   '') AS phone,
  coalesce(c.address, '') AS address,
  t.municipality,
  t.county
FROM tax_latest AS t
LEFT JOIN contact  AS c
  ON c.registry_code = t.registry_code