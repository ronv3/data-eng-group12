{{ config(materialized='table') }}

WITH raw AS (
  SELECT
    period_date,
    ingested_at,

    -- core accommodation fields (Estonian keys)
    JSONExtractString(raw_json, 'Turismiobjekti nimi')          AS name,
    JSONExtractString(raw_json, 'Kategooria')                    AS category,
    JSONExtractString(raw_json, 'Tüüp')                          AS type,
    JSONExtractString(raw_json, 'Turismiobjekti koduleht')       AS property_website,
    JSONExtractString(raw_json, 'Broneerimise link')             AS booking_link,
    JSONExtractString(raw_json, 'facebook')                      AS facebook,
    JSONExtractString(raw_json, 'instagram')                     AS instagram,
    JSONExtractString(raw_json, 'tiktok')                        AS tiktok,

    JSONExtractString(raw_json, 'Hooaeg')                        AS season,
    JSONExtractString(raw_json, 'Hooaja periood')                AS season_period,
    JSONExtractString(raw_json, 'Lahtiolekuaeg')                 AS opening_hours_type,
    JSONExtractString(raw_json, 'Avatud ajad')                   AS opening_times,

    JSONExtractString(raw_json, 'Turismiobjekti aadress')        AS address,
    JSONExtractString(raw_json, 'Piirkond')                      AS region,
    JSONExtractString(raw_json, 'Saared')                        AS island,

    -- company contact fields (from housing)
    JSONExtractString(raw_json, 'Ettevõtte nimi')                AS company_name,
    JSONExtractString(raw_json, 'Ettevõtte koduleht')            AS company_website,
    JSONExtractString(raw_json, 'Ettevõtte e-post')              AS company_email,
    JSONExtractString(raw_json, 'Ettevõtte telefon')             AS company_phone,
    JSONExtractString(raw_json, 'Ettevõtte aadress')             AS company_address,

    -- numeric metrics (these fields are strings in JSON; JSONExtractString works)
    toInt32OrNull(JSONExtractString(raw_json, 'Tärnid'))                              AS stars_raw,
    toInt32OrNull(JSONExtractString(raw_json, 'Tubade arv kokku'))                    AS room_count_total,
    toInt32OrNull(JSONExtractString(raw_json, 'Voodikohad'))                          AS bed_count,
    toInt32OrNull(JSONExtractString(raw_json, 'Voodikohtade arv kõrghooajal'))        AS beds_peak,
    toInt32OrNull(JSONExtractString(raw_json, 'Voodikohtade arv madalhooajal'))       AS beds_low,
    toInt32OrNull(JSONExtractString(raw_json, 'Haagissuvila kohtade arv'))            AS caravan_spots,
    toInt32OrNull(JSONExtractString(raw_json, 'Telkimiskohtade arv'))                 AS tent_spots,

    -- registry code can be a JSON number or a string; normalize from RAW
    replaceRegexpAll(
      replaceRegexpAll(JSONExtractRaw(raw_json, 'Ettevõtte registrikood'), '^"|"$', ''),
      '\\.0$', ''
    ) AS property_bk_raw
  FROM {{ source('bronze','housing_raw') }}
)

SELECT
  period_date,
  ingested_at,

  nullIf(property_bk_raw, '')        AS property_bk,
  nullIf(name, '')                   AS name,
  nullIf(category, '')               AS category,
  nullIf(type, '')                   AS type,
  stars_raw                          AS stars,
  nullIf(booking_link, '')           AS booking_link,

  coalesce(room_count_total, 0)      AS rooms_cnt,
  coalesce(bed_count, 0)             AS beds_total,
  coalesce(beds_peak, 0)             AS beds_high_season,
  coalesce(beds_low, 0)              AS beds_low_season,
  coalesce(caravan_spots, 0)         AS caravan_spots,
  coalesce(tent_spots, 0)            AS tent_spots,

  nullIf(season, '')                 AS seasonal_flag_raw,
  nullIf(season_period, '')          AS season_period,
  nullIf(opening_hours_type, '')     AS opening_hours_type,
  nullIf(opening_times, '')          AS opening_times,

  nullIf(address, '')                AS address,
  nullIf(region, '')                 AS region,
  nullIf(island, '')                 AS island,

  nullIf(company_name, '')           AS company_name,
  nullIf(company_website, '')        AS company_website,
  nullIf(company_email, '')          AS company_email,
  nullIf(company_phone, '')          AS company_phone,
  nullIf(company_address, '')        AS company_address,

  nullIf(facebook, '')               AS facebook,
  nullIf(instagram, '')              AS instagram,
  nullIf(tiktok, '')                 AS tiktok

FROM raw
WHERE (name != '' OR property_bk_raw != '');
