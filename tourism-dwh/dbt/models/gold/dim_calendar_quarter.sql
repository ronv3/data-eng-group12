{{ config(
  schema='gold',
  materialized='table',
  engine='MergeTree()',
  order_by=['quarter_sk']
) }}

WITH bounds AS (
  SELECT
    toDate('2000-01-01') AS d_start,
    toDate('2035-12-31') AS d_end
),
span AS (
  /* number of whole 3-month steps inclusive */
  SELECT
    d_start,
    greatest(0, intDiv(dateDiff('month', d_start, d_end), 3)) AS q_span
  FROM bounds
),
series AS (
  /* Finite generator: 0..q_span, then step by 3 months */
  SELECT
    addMonths(d_start, q * 3) AS quarter_start
  FROM span
  ARRAY JOIN range(q_span + 1) AS q
)
SELECT
  toUInt32(toYear(quarter_start) * 10 + toQuarter(quarter_start)) AS quarter_sk,
  toDateTime(quarter_start)                                        AS quarter_start
FROM series
ORDER BY quarter_start