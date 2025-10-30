{{ config(materialized='table') }}

WITH base AS (
  SELECT
    -- same robust registry-code normalization
    nullIf(
      replaceRegexpAll(
        replaceRegexpAll(JSONExtractRaw(raw_json, 'Ettevõtte registrikood'), '^"|"$', ''),
        '\\.0$', ''
      ),
      ''
    ) AS property_bk,
    coalesce(JSONExtractString(raw_json, 'Omadused ja üldine varustus'), '') AS features_general,
    coalesce(JSONExtractString(raw_json, 'Teemaviited'), '')                  AS features_tags
  FROM {{ source('bronze','housing_raw') }}
),
split AS (
  SELECT
    property_bk,
    arrayFilter(x -> x != '',
      arrayMap(x -> trim(BOTH ' ' FROM x),
        arrayConcat(
          splitByChar(',', features_general),
          splitByChar(',', features_tags)
        )
      )
    ) AS feature_arr
  FROM base
  WHERE property_bk IS NOT NULL
)
SELECT
  property_bk,
  feature AS feature_name
FROM split
ARRAY JOIN feature_arr AS feature
GROUP BY property_bk, feature
ORDER BY property_bk, feature;
