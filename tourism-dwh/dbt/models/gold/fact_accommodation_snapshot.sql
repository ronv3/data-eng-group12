{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by=['quarter_sk']
) }}

WITH base AS (
    SELECT
        -- BKs & normalized strings
        lowerUTF8(toString(property_bk))                                        AS property_bk_norm,
        lowerUTF8(toString(coalesce(name, '')))                                  AS name_norm,
        lowerUTF8(nullIf(replaceRegexpAll(toString(region), '^[[:space:]]+|[[:space:]]+$', ''), ''))  AS region_norm,
        lowerUTF8(nullIf(replaceRegexpAll(toString(island), '^[[:space:]]+|[[:space:]]+$', ''), ''))  AS island_norm,

        -- metrics
        toUInt32(coalesce(rooms_cnt, 0))     AS rooms_cnt,
        toUInt32(coalesce(beds_total, 0))    AS beds_total,
        toUInt32(coalesce(caravan_spots, 0)) AS caravan_spots,
        toUInt32(coalesce(tent_spots, 0))    AS tent_spots,

        -- reporting quarter (already aligned in silver)
        toUInt32(reporting_quarter_sk)      AS quarter_sk,
        toDateTime(reporting_quarter_start) AS quarter_start_dt,

        -- company & accommodation BKs (match dim construction)
        lowerUTF8(replaceRegexpAll(toString(registry_code), '\\D', '')) AS company_id,
        lowerUTF8(toString(concat(toString(property_bk), '|', coalesce(name, '')))) AS accommodation_bk,

        toDateTime(ingested_at) AS ingested_at
    FROM {{ ref('stg_housing_accommodation') }}
    WHERE property_bk IS NOT NULL
),

q AS (
    SELECT
        cq.quarter_sk,
        toDateTime(addDays(addMonths(cq.quarter_start, 3), -1)) AS quarter_end_dt
    FROM {{ ref('dim_calendar_quarter') }} AS cq
),

-- expose join keys explicitly to avoid CH identifier issues
acc_asof AS (
    SELECT
        b.accommodation_bk                              AS acc_bk,
        b.quarter_sk                                    AS q_sk,
        coalesce(
          argMaxIf(da.accommodation_sk, da.effective_from,
                   (q.quarter_end_dt >= da.effective_from) AND (q.quarter_end_dt < da.effective_to)),
          argMin(da.accommodation_sk, da.effective_from)
        )                                               AS accommodation_sk
    FROM base AS b
    INNER JOIN q ON q.quarter_sk = b.quarter_sk
    LEFT JOIN {{ ref('dim_accommodation') }} AS da
           ON da.accommodation_bk = b.accommodation_bk
    GROUP BY acc_bk, q_sk
),

co_asof AS (
    SELECT
        b.company_id                                    AS co_id,
        b.quarter_sk                                    AS q_sk,
        coalesce(
          argMaxIf(dc.company_sk, dc.effective_from,
                   (q.quarter_end_dt >= dc.effective_from) AND (q.quarter_end_dt < dc.effective_to)),
          argMin(dc.company_sk, dc.effective_from)
        )                                               AS company_sk
    FROM base AS b
    INNER JOIN q ON q.quarter_sk = b.quarter_sk
    LEFT JOIN {{ ref('dim_company') }} AS dc
           ON dc.company_id = b.company_id
    GROUP BY co_id, q_sk
),

geo_lu AS (
    -- we model geo only by (region, island)
    SELECT region, island, geo_sk
    FROM {{ ref('dim_geography') }}
)

SELECT
    a.accommodation_sk,
    c.company_sk,
    b.quarter_sk,
    g.geo_sk,

    sum(b.rooms_cnt)      AS rooms_cnt,
    sum(b.beds_total)     AS beds_total,
    sum(b.caravan_spots)  AS caravan_spots,
    sum(b.tent_spots)     AS tent_spots
FROM base AS b
LEFT JOIN acc_asof AS a
       ON a.acc_bk = b.accommodation_bk
      AND a.q_sk   = b.quarter_sk
LEFT JOIN co_asof  AS c
       ON c.co_id  = b.company_id
      AND c.q_sk   = b.quarter_sk
LEFT JOIN geo_lu   AS g
       ON g.region = b.region_norm
      AND g.island = b.island_norm
GROUP BY
    a.accommodation_sk,
    c.company_sk,
    b.quarter_sk,
    g.geo_sk