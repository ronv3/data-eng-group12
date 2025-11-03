{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by=['quarter_sk']
) }}

WITH base AS (
    SELECT
        -- business key aligned with dim_company
        lowerUTF8(replaceRegexpAll(toString(registry_code), '\\D', '')) AS company_id,

        -- measures
        toFloat64(coalesce(turnover_eur,     0)) AS turnover_eur,
        toFloat64(coalesce(state_taxes_eur,  0)) AS state_taxes_eur,
        toFloat64(coalesce(labour_taxes_eur, 0)) AS labour_taxes_eur,
        toInt64(  coalesce(employees_cnt,    0)) AS employees_cnt,

        -- quarter_sk: year is Nullable(Int32) -> make it non-null Int32, quarter is UInt8 -> cast to Int32
        toUInt32( coalesce(year, 0) * 10 + toInt32(quarter) )          AS quarter_sk,

        -- keep ingest timestamp
        toDateTime(ingested_at) AS ingested_at
    FROM {{ ref('stg_tax_company_quarter') }}
    WHERE registry_code IS NOT NULL
      AND quarter IN (1,2,3,4)
),

q AS (
    SELECT
        cq.quarter_sk,
        cq.quarter_start,
        toDateTime(addDays(addMonths(cq.quarter_start, 3), -1)) AS quarter_end_dt
    FROM {{ ref('dim_calendar_quarter') }} AS cq
),

co_asof AS (
    SELECT
        b.company_id AS co_id,
        b.quarter_sk AS q_sk,
        coalesce(
            argMaxIf(dc.company_sk, dc.effective_from,
                     (q.quarter_end_dt >= dc.effective_from) AND (q.quarter_end_dt < dc.effective_to)),
            argMin(dc.company_sk, dc.effective_from)
        ) AS company_sk
    FROM base AS b
    INNER JOIN q ON q.quarter_sk = b.quarter_sk
    LEFT JOIN {{ ref('dim_company') }} AS dc
           ON dc.company_id = b.company_id
    GROUP BY co_id, q_sk
)

SELECT
    c.company_sk,
    b.quarter_sk,
    sum(b.turnover_eur)     AS turnover_eur,
    sum(b.state_taxes_eur)  AS state_taxes_eur,
    sum(b.labour_taxes_eur) AS labour_taxes_eur,
    sum(b.employees_cnt)    AS employees_cnt
FROM base AS b
LEFT JOIN co_asof AS c
       ON c.co_id = b.company_id
      AND c.q_sk  = b.quarter_sk
GROUP BY
    c.company_sk,
    b.quarter_sk