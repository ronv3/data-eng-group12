{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_SILVER','silver')) }}

-- 1) Read bronze, normalize NaN -> null once + capture ingestion time
WITH src AS (
  SELECT
    replaceAll(raw_json, 'NaN', 'null') AS raw_json_sane,
    coalesce(ingested_at, now())        AS ingested_at
  FROM {{ source('bronze','tax_raw') }}
),

-- 2) Parse fields to wide form (year + 4 quarters)
base AS (
  SELECT
    /* registry code as text without quotes/.0 */
    nullIf(
      replaceRegexpAll(
        replaceRegexpAll(JSONExtractRaw(raw_json_sane, 'Registry code'), '^"|"$',''),
        '\\.0$',''
      ), ''
    ) AS registry_code_raw,

    /* Year */
    toInt32OrNull(
      nullIf(
        replaceRegexpAll(
          replaceRegexpAll(JSONExtractRaw(raw_json_sane, 'Year'), '^"|"$',''),
          '\\.0$',''
        ), ''
      )
    ) AS year,

    nullIf(JSONExtractString(raw_json_sane, 'Name'),     '') AS company_name,
    nullIf(JSONExtractString(raw_json_sane, 'Activity'), '') AS activity,
    JSONExtractString(raw_json_sane, 'County')               AS county_muni_raw,

    /* numeric measures per quarter (robust to numeric-or-text) */
    coalesce(JSONExtractFloat(raw_json_sane, 'Turnover I qtr' ), toFloat64OrNull(JSONExtractString(raw_json_sane, 'Turnover I qtr' ))) AS t1,
    coalesce(JSONExtractFloat(raw_json_sane, 'State taxes I qtr'), toFloat64OrNull(JSONExtractString(raw_json_sane, 'State taxes I qtr'))) AS s1,
    coalesce(JSONExtractFloat(raw_json_sane, 'Labour taxes and payments I qtr'), toFloat64OrNull(JSONExtractString(raw_json_sane, 'Labour taxes and payments I qtr'))) AS l1,
    coalesce(JSONExtractFloat(raw_json_sane, 'Number of employees I qtr'),        toFloat64OrNull(JSONExtractString(raw_json_sane, 'Number of employees I qtr')))        AS e1,

    coalesce(JSONExtractFloat(raw_json_sane, 'Turnover II qtr' ), toFloat64OrNull(JSONExtractString(raw_json_sane, 'Turnover II qtr' ))) AS t2,
    coalesce(JSONExtractFloat(raw_json_sane, 'State taxes II qtr'), toFloat64OrNull(JSONExtractString(raw_json_sane, 'State taxes II qtr'))) AS s2,
    coalesce(JSONExtractFloat(raw_json_sane, 'Labour taxes and payments II qtr'), toFloat64OrNull(JSONExtractString(raw_json_sane, 'Labour taxes and payments II qtr'))) AS l2,
    coalesce(JSONExtractFloat(raw_json_sane, 'Number of employees II qtr'),        toFloat64OrNull(JSONExtractString(raw_json_sane, 'Number of employees II qtr')))        AS e2,

    coalesce(JSONExtractFloat(raw_json_sane, 'Turnover III qtr'), toFloat64OrNull(JSONExtractString(raw_json_sane, 'Turnover III qtr'))) AS t3,
    coalesce(JSONExtractFloat(raw_json_sane, 'State taxes III qtr'), toFloat64OrNull(JSONExtractString(raw_json_sane, 'State taxes III qtr'))) AS s3,
    coalesce(JSONExtractFloat(raw_json_sane, 'Labour taxes and payments III qtr'), toFloat64OrNull(JSONExtractString(raw_json_sane, 'Labour taxes and payments III qtr'))) AS l3,
    coalesce(JSONExtractFloat(raw_json_sane, 'Number of employees III qtr'),        toFloat64OrNull(JSONExtractString(raw_json_sane, 'Number of employees III qtr')))        AS e3,

    coalesce(JSONExtractFloat(raw_json_sane, 'Turnover IV qtr' ), toFloat64OrNull(JSONExtractString(raw_json_sane, 'Turnover IV qtr' ))) AS t4,
    coalesce(JSONExtractFloat(raw_json_sane, 'State taxes IV qtr'), toFloat64OrNull(JSONExtractString(raw_json_sane, 'State taxes IV qtr'))) AS s4,
    coalesce(JSONExtractFloat(raw_json_sane, 'Labour taxes and payments IV qtr'), toFloat64OrNull(JSONExtractString(raw_json_sane, 'Labour taxes and payments IV qtr'))) AS l4,
    coalesce(JSONExtractFloat(raw_json_sane, 'Number of employees IV qtr'),        toFloat64OrNull(JSONExtractString(raw_json_sane, 'Number of employees IV qtr')))        AS e4,

    ingested_at
  FROM src
),

-- 3) Clean registry + county/municipality and unpivot quarters
unpivot AS (
  SELECT
    nullIf(registry_code_raw, '') AS registry_code,

    /* county = before '(' ; municipality = inside '( ... )' ; both trimmed */
    nullIf(trim(BOTH ' ' FROM replaceRegexpAll(coalesce(county_muni_raw,''), '\\(.*$', '')), '')                               AS county,
    nullIf(trim(BOTH ' ' FROM replaceRegexpAll(coalesce(county_muni_raw,''), '.*\\(\\s*([^\\)]+)\\s*\\).*$', '\\1')), '')     AS municipality,

    nullIf(company_name,'') AS company_name,
    nullIf(activity,'')     AS activity,

    year,
    arrayJoin([(1,t1,s1,l1,e1),(2,t2,s2,l2,e2),(3,t3,s3,l3,e3),(4,t4,s4,l4,e4)]) AS q,
    ingested_at
  FROM base
)

SELECT
  registry_code,
  year,
  q.1 AS quarter,
  toDate(concat(toString(year), '-', multiIf(q.1 = 1, '01', q.1 = 2, '04', q.1 = 3, '07', '10'), '-01')) AS quarter_start,

  q.2 AS turnover_eur,
  q.3 AS state_taxes_eur,
  q.4 AS labour_taxes_eur,
  CAST(q.5 AS Nullable(Int32)) AS employees_cnt,

  company_name,
  activity,
  county,
  municipality,
  ingested_at
FROM unpivot
WHERE q.2 IS NOT NULL
   OR q.3 IS NOT NULL
   OR q.4 IS NOT NULL
   OR q.5 IS NOT NULL