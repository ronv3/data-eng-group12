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
    ) AS registry_code,

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

    /* numeric measures per quarter (simple JSONExtractFloat) */
    JSONExtractFloat(raw_json_sane, 'Turnover I qtr' )                         AS t1,
    JSONExtractFloat(raw_json_sane, 'State taxes I qtr')                       AS s1,
    JSONExtractFloat(raw_json_sane, 'Labour taxes and payments I qtr')        AS l1,
    JSONExtractFloat(raw_json_sane, 'Number of employees I qtr')              AS e1,

    JSONExtractFloat(raw_json_sane, 'Turnover II qtr' )                        AS t2,
    JSONExtractFloat(raw_json_sane, 'State taxes II qtr')                      AS s2,
    JSONExtractFloat(raw_json_sane, 'Labour taxes and payments II qtr')       AS l2,
    JSONExtractFloat(raw_json_sane, 'Number of employees II qtr')             AS e2,

    JSONExtractFloat(raw_json_sane, 'Turnover III qtr')                        AS t3,
    JSONExtractFloat(raw_json_sane, 'State taxes III qtr')                     AS s3,
    JSONExtractFloat(raw_json_sane, 'Labour taxes and payments III qtr')      AS l3,
    JSONExtractFloat(raw_json_sane, 'Number of employees III qtr')            AS e3,

    JSONExtractFloat(raw_json_sane, 'Turnover IV qtr' )                        AS t4,
    JSONExtractFloat(raw_json_sane, 'State taxes IV qtr')                      AS s4,
    JSONExtractFloat(raw_json_sane, 'Labour taxes and payments IV qtr')       AS l4,
    JSONExtractFloat(raw_json_sane, 'Number of employees IV qtr')             AS e4,

    ingested_at
  FROM src
),

-- 3) Add clean county / municipality once
cleaned AS (
  SELECT
    registry_code,
    year,
    nullIf(company_name,'') AS company_name,
    nullIf(activity,'')     AS activity,

    /* county = before '(' ; municipality = inside '( ... )' ; both trimmed */
    nullIf(
      trim(BOTH ' ' FROM replaceRegexpAll(coalesce(county_muni_raw,''), '\\(.*$', '')),
      ''
    ) AS county,
    nullIf(
      trim(BOTH ' ' FROM replaceRegexpAll(coalesce(county_muni_raw,''), '.*\\(\\s*([^\\)]+)\\s*\\).*$', '\\1')),
      ''
    ) AS municipality,

    t1, s1, l1, e1,
    t2, s2, l2, e2,
    t3, s3, l3, e3,
    t4, s4, l4, e4,

    ingested_at
  FROM base
)

-- 4) One SELECT per quarter, UNION ALL
SELECT
  registry_code,
  year,
  1 AS quarter,
  toDate(concat(toString(year), '-01-01')) AS quarter_start,
  t1 AS turnover_eur,
  s1 AS state_taxes_eur,
  l1 AS labour_taxes_eur,
  CAST(e1 AS Nullable(Int32)) AS employees_cnt,
  company_name,
  activity,
  county,
  municipality,
  ingested_at
FROM cleaned
WHERE t1 IS NOT NULL
   OR s1 IS NOT NULL
   OR l1 IS NOT NULL
   OR e1 IS NOT NULL

UNION ALL

SELECT
  registry_code,
  year,
  2 AS quarter,
  toDate(concat(toString(year), '-04-01')) AS quarter_start,
  t2 AS turnover_eur,
  s2 AS state_taxes_eur,
  l2 AS labour_taxes_eur,
  CAST(e2 AS Nullable(Int32)) AS employees_cnt,
  company_name,
  activity,
  county,
  municipality,
  ingested_at
FROM cleaned
WHERE t2 IS NOT NULL
   OR s2 IS NOT NULL
   OR l2 IS NOT NULL
   OR e2 IS NOT NULL

UNION ALL

SELECT
  registry_code,
  year,
  3 AS quarter,
  toDate(concat(toString(year), '-07-01')) AS quarter_start,
  t3 AS turnover_eur,
  s3 AS state_taxes_eur,
  l3 AS labour_taxes_eur,
  CAST(e3 AS Nullable(Int32)) AS employees_cnt,
  company_name,
  activity,
  county,
  municipality,
  ingested_at
FROM cleaned
WHERE t3 IS NOT NULL
   OR s3 IS NOT NULL
   OR l3 IS NOT NULL
   OR e3 IS NOT NULL

UNION ALL

SELECT
  registry_code,
  year,
  4 AS quarter,
  toDate(concat(toString(year), '-10-01')) AS quarter_start,
  t4 AS turnover_eur,
  s4 AS state_taxes_eur,
  l4 AS labour_taxes_eur,
  CAST(e4 AS Nullable(Int32)) AS employees_cnt,
  company_name,
  activity,
  county,
  municipality,
  ingested_at
FROM cleaned
WHERE t4 IS NOT NULL
   OR s4 IS NOT NULL
   OR l4 IS NOT NULL
   OR e4 IS NOT NULL
