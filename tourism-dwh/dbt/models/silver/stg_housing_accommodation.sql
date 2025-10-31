{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_SILVER','silver')) }}

WITH src AS (
  SELECT
    period_date,
    ingested_at,
    replaceAll(raw_json, 'NaN', 'null') AS raw_json_sane
  FROM {{ source('bronze','housing_raw') }}
),

-- Try several header variants; tolerate quoted/raw and Excel ".0"
reg_code_raw AS (
  SELECT
    period_date,
    ingested_at,
    coalesce(
      nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte registrikood'), ''),
      nullIf(JSONExtractString(raw_json_sane, 'ettevõtte registrikood'), ''),
      nullIf(JSONExtractString(raw_json_sane, 'Ettev\u00f5tte registrikood'), ''),
      nullIf(JSONExtractString(raw_json_sane, 'Ettevotte registrikood'), ''),
      nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte regnr'), ''),
      nullIf(JSONExtractString(raw_json_sane, 'Registrikood'), ''),
      nullIf(JSONExtractString(raw_json_sane, 'Reg nr'), ''),
      nullIf(
        replaceRegexpAll(
          replaceRegexpAll(JSONExtractRaw(raw_json_sane, 'Ettevõtte registrikood'), '^"|"$', ''),
          '\\.0$', ''
        ), ''
      )
    ) AS registry_code_raw,
    raw_json_sane
  FROM src
)

SELECT
  period_date,
  ingested_at,

  /* Keep property_bk for backward compatibility (string form, no quotes, drop .0) */
  nullIf(
    replaceRegexpAll(
      replaceRegexpAll(coalesce(registry_code_raw, ''), '^"|"$', ''),
      '\\.0$', ''
    ),
    ''
  ) AS property_bk,

  /* NEW: digits-only registry code, ideal for joins */
  nullIf(
    replaceRegexpAll(toString(registry_code_raw), '[^0-9]', ''),
    ''
  ) AS registry_code,

  -- existing fields
  nullIf(JSONExtractString(raw_json_sane, 'Turismiobjekti nimi'), '')  AS name,
  nullIf(JSONExtractString(raw_json_sane, 'Kategooria'), '')            AS category,
  nullIf(JSONExtractString(raw_json_sane, 'Tüüp'), '')                  AS type,
  toInt32OrNull(JSONExtractString(raw_json_sane, 'Tärnid'))             AS stars,
  nullIf(JSONExtractString(raw_json_sane, 'Broneerimise link'), '')     AS booking_link,

  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Tubade arv kokku')), 0)              AS rooms_cnt,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Voodikohad')), 0)                    AS beds_total,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Voodikohtade arv kõrghooajal')), 0)  AS beds_high_season,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Voodikohtade arv madalhooajal')), 0) AS beds_low_season,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Haagissuvila kohtade arv')), 0)      AS caravan_spots,
  coalesce(toInt32OrNull(JSONExtractString(raw_json_sane, 'Telkimiskohtade arv')), 0)           AS tent_spots,

  nullIf(JSONExtractString(raw_json_sane, 'Hooaeg'), '')                 AS seasonal_flag_raw,
  nullIf(JSONExtractString(raw_json_sane, 'Hooaja periood'), '')         AS season_period,
  nullIf(JSONExtractString(raw_json_sane, 'Lahtiolekuaeg'), '')          AS opening_hours_type,
  nullIf(JSONExtractString(raw_json_sane, 'Avatud ajad'), '')            AS opening_times,

  nullIf(JSONExtractString(raw_json_sane, 'Turismiobjekti aadress'), '') AS address,
  nullIf(JSONExtractString(raw_json_sane, 'Piirkond'), '')               AS region,
  nullIf(JSONExtractString(raw_json_sane, 'Saared'), '')                 AS island,

  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte nimi'), '')         AS company_name,
  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte koduleht'), '')     AS company_website,
  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte e-post'), '')       AS company_email,
  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte telefon'), '')      AS company_phone,
  nullIf(JSONExtractString(raw_json_sane, 'Ettevõtte aadress'), '')      AS company_address,

  nullIf(JSONExtractString(raw_json_sane, 'facebook'), '')               AS facebook,
  nullIf(JSONExtractString(raw_json_sane, 'instagram'), '')              AS instagram,
  nullIf(JSONExtractString(raw_json_sane, 'tiktok'), '')                 AS tiktok
FROM reg_code_raw
WHERE (JSONExtractString(raw_json_sane, 'Turismiobjekti nimi') != '')
   OR (registry_code_raw IS NOT NULL AND registry_code_raw != '')