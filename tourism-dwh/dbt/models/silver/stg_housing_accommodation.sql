{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_SILVER','silver')) }}

WITH src AS (
  SELECT
    period_date,
    ingested_at,
    /* sanitize non-standard JSON tokens so ClickHouse can parse */
    replaceAll(raw_json, 'NaN', 'null') AS raw_json_sane
  FROM {{ source('bronze','housing_raw') }}
)

SELECT
  period_date,
  ingested_at,

  /* use raw_json_sane everywhere below */
  nullIf(
    replaceRegexpAll(
      replaceRegexpAll(JSONExtractRaw(raw_json_sane, 'Ettevõtte registrikood'), '^"|"$', ''),
      '\\.0$', ''
    ),
    ''
  )                                   AS property_bk,
  nullIf(JSONExtractString(raw_json_sane, 'Turismiobjekti nimi'), '')            AS name,
  nullIf(JSONExtractString(raw_json_sane, 'Kategooria'), '')                      AS category,
  nullIf(JSONExtractString(raw_json_sane, 'Tüüp'), '')                            AS type,
  toInt32OrNull(JSONExtractString(raw_json_sane, 'Tärnid'))                       AS stars,
  nullIf(JSONExtractString(raw_json_sane, 'Broneerimise link'), '')               AS booking_link,

  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Tubade arv kokku')), 0)                 AS rooms_cnt,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Voodikohad')), 0)                       AS beds_total,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Voodikohtade arv kõrghooajal')), 0)     AS beds_high_season,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Voodikohtade arv madalhooajal')), 0)    AS beds_low_season,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Haagissuvila kohtade arv')), 0)         AS caravan_spots,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Telkimiskohtade arv')), 0)              AS tent_spots,

  nullIf(JSONExtractString(raw_json_sane, 'Hooaeg'), '')                     AS seasonal_flag_raw,
  nullIf(JSONExtractString(raw_json_sane, 'Hooaja periood'), '')             AS season_period,
  nullIf(JSONExtractString(raw_json_sane, 'Lahtiolekuaeg'), '')              AS opening_hours_type,
  nullIf(JSONExtractString(raw_json_sane, 'Avatud ajad'), '')                AS opening_times,

  nullIf(JSONExtractString(raw_json_sane, 'Turismiobjekti aadress'), '')     AS address,
  nullIf(JSONExtractString(raw_json_sane, 'Piirkond'), '')                   AS region,
  nullIf(JSONExtractString(raw_json_sane, 'Saared'), '')                     AS island,

  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte nimi'), '')             AS company_name,
  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte koduleht'), '')         AS company_website,
  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte e-post'), '')           AS company_email,
  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte telefon'), '')          AS company_phone,
  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte aadress'), '')          AS company_address,

  nullIf(JSONExtractString(raw_json_sane, 'facebook'), '')                   AS facebook,
  nullIf(JSONExtractString(raw_json_sane, 'instagram'), '')                  AS instagram,
  nullIf(JSONExtractString(raw_json_sane, 'tiktok'), '')                     AS tiktok
FROM src
WHERE (JSONExtractString(raw_json_sane, 'Turismiobjekti nimi') != '')
   OR (JSONExtractRaw(raw_json_sane, 'Ettevõtte registrikood')   != '')
