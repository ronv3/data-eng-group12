{{ config(materialized='table', schema='silver') }}

WITH raw AS (
  SELECT
    period_date,
    ingested_at,
    /* ClickHouse JSONExtract* reads from JSON string */
    toInt32OrNull(JSONExtractString(raw_json, 'Voodikohad'))                          AS bed_count,
    toInt32OrNull(JSONExtractString(raw_json, 'Tubade arv kokku'))                    AS room_count_total,
    toInt32OrNull(JSONExtractString(raw_json, 'Voodikohtade arv kõrghooajal'))        AS beds_peak,
    toInt32OrNull(JSONExtractString(raw_json, 'Voodikohtade arv madalhooajal'))       AS beds_low,
    toInt32OrNull(JSONExtractString(raw_json, 'Haagissuvila kohtade arv'))            AS caravan_spots,
    toInt32OrNull(JSONExtractString(raw_json, 'Telkimiskohtade arv'))                 AS tent_spots,
    JSONExtractString(raw_json, 'Hooaeg')                                             AS season,
    JSONExtractString(raw_json, 'Hooaja periood')                                     AS season_period,
    JSONExtractString(raw_json, 'Lahtiolekuaeg')                                      AS opening_hours_type,
    JSONExtractString(raw_json, 'Avatud ajad')                                        AS opening_times
  FROM {{ source('bronze','housing_raw') }}
)
SELECT
  period_date,
  ingested_at,
  coalesce(bed_count, 0)        AS bed_count,
  coalesce(room_count_total, 0) AS room_count_total,
  coalesce(beds_peak, 0)        AS beds_peak,
  coalesce(beds_low, 0)         AS beds_low,
  coalesce(caravan_spots, 0)    AS caravan_spots,
  coalesce(tent_spots, 0)       AS tent_spots,
  season,
  season_period,
  opening_hours_type,
  opening_times
FROM raw;
