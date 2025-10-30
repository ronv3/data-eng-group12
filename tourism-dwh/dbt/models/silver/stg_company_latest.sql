{{ config(materialized='table') }}

-- Build the canonical list of companies from tax silver,
-- then enrich with whatever contact info we can glean from housing.
WITH tax_latest AS (
  SELECT
    registry_code,
    anyLast(company_name)  AS name,
    anyLast(activity)      AS activity,
    anyLast(municipality)  AS municipality,
    anyLast(county)        AS county
  FROM {{ ref('stg_tax_company_quarter') }}
  GROUP BY registry_code
),
contact AS (
  SELECT
    property_bk                         AS registry_code,
    anyLast(company_website)            AS website,
    anyLast(company_email)              AS email,
    anyLast(company_phone)              AS phone,
    anyLast(company_address)            AS address
  FROM {{ ref('stg_housing_accommodation') }}
  WHERE property_bk IS NOT NULL
  GROUP BY property_bk
)
SELECT
  t.registry_code,
  t.name,
  t.activity,
  coalesce(c.website,   '') AS website,
  coalesce(c.email,     '') AS email,
  coalesce(c.phone,     '') AS phone,
  coalesce(c.address,   '') AS address,
  t.municipality,
  t.county
FROM tax_latest t
LEFT JOIN contact c USING (registry_code);
