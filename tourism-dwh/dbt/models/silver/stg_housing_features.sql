{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_SILVER','silver')) }}

WITH src AS (
  SELECT replaceAll(raw_json, 'NaN', 'null') AS raw_json_sane
  FROM {{ source('bronze','housing_raw') }}
),
base AS (
  SELECT
    nullIf(
      replaceRegexpAll(
        replaceRegexpAll(JSONExtractRaw(raw_json_sane,'Ettevõtte registrikood'), '^"|"$',''),
        '\\.0$',''
      ),
      ''
    ) AS property_bk,
    nullIf(JSONExtractString(raw_json_sane,'Turismiobjekti nimi'),'') AS accommodation_name,
    replaceRegexpAll(coalesce(JSONExtractString(raw_json_sane,'Omadused ja üldine varustus'),''),'[;\\n\\r]+',',') AS fgen,
    replaceRegexpAll(coalesce(JSONExtractString(raw_json_sane,'Teemaviited'),''),'[;\\n\\r]+',',')                 AS ftags
  FROM src
)
SELECT
  property_bk,
  accommodation_name,
  feature_name
FROM (
  SELECT property_bk, accommodation_name, trim(BOTH ' ' FROM arrayJoin(splitByChar(',', fgen)))  AS feature_name
  FROM base WHERE property_bk IS NOT NULL AND accommodation_name IS NOT NULL

  UNION ALL

  SELECT property_bk, accommodation_name, trim(BOTH ' ' FROM arrayJoin(splitByChar(',', ftags))) AS feature_name
  FROM base WHERE property_bk IS NOT NULL AND accommodation_name IS NOT NULL
)
WHERE feature_name <> ''
GROUP BY property_bk, accommodation_name, feature_name
