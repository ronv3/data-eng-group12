{{ config(materialized='table', schema=env_var('CLICKHOUSE_DB_SILVER', 'silver')) }}

WITH src AS (
    SELECT
        raw_json,
        ingested_at
    FROM {{ source('bronze','tax_raw') }}
),
base AS (
    SELECT
        coalesce(
            toString(JSONExtractString(raw_json, 'Registry code')),
            toString(JSONExtractInt(raw_json, 'Registry code')),
            toString(JSONExtractUInt(raw_json, 'Registry code')),
            replaceAll(JSONExtractRaw(raw_json, 'Registry code'), '"', '')
        )                                                     AS registry_code,
        JSONExtractString(raw_json, 'Name')                   AS company_name,
        JSONExtractString(raw_json, 'Activity')               AS activity,
        JSONExtractString(raw_json, 'County')                 AS county_raw,
        toInt32OrNull(JSONExtractString(raw_json, 'Year'))    AS year_val,
        array('I','II','III','IV')                            AS q_labels,
        [
          toFloat64OrNull(JSONExtractString(raw_json,'Turnover I qtr')),
          toFloat64OrNull(JSONExtractString(raw_json,'Turnover II qtr')),
          toFloat64OrNull(JSONExtractString(raw_json,'Turnover III qtr')),
          toFloat64OrNull(JSONExtractString(raw_json,'Turnover IV qtr'))
        ] AS turnover_arr,
        [
          toFloat64OrNull(JSONExtractString(raw_json,'State taxes I qtr')),
          toFloat64OrNull(JSONExtractString(raw_json,'State taxes II qtr')),
          toFloat64OrNull(JSONExtractString(raw_json,'State taxes III qtr')),
          toFloat64OrNull(JSONExtractString(raw_json,'State taxes IV qtr'))
        ] AS state_taxes_arr,
        [
          toFloat64OrNull(JSONExtractString(raw_json,'Labour taxes and payments I qtr')),
          toFloat64OrNull(JSONExtractString(raw_json,'Labour taxes and payments II qtr')),
          toFloat64OrNull(JSONExtractString(raw_json,'Labour taxes and payments III qtr')),
          toFloat64OrNull(JSONExtractString(raw_json,'Labour taxes and payments IV qtr'))
        ] AS labour_taxes_arr,
        [
          toInt32OrNull(JSONExtractString(raw_json,'Number of employees I qtr')),
          toInt32OrNull(JSONExtractString(raw_json,'Number of employees II qtr')),
          toInt32OrNull(JSONExtractString(raw_json,'Number of employees III qtr')),
          toInt32OrNull(JSONExtractString(raw_json,'Number of employees IV qtr'))
        ] AS employees_arr
    FROM src
),
exploded AS (
    SELECT
        registry_code,
        company_name,
        activity,
        county_raw,
        year_val,
        q_lbl,
        idx,
        turnover,
        state_taxes,
        labour_taxes,
        employees
    FROM base
    ARRAY JOIN
        arrayEnumerate(q_labels) AS idx,
        q_labels AS q_lbl,
        turnover_arr AS turnover,
        state_taxes_arr AS state_taxes,
        labour_taxes_arr AS labour_taxes,
        employees_arr AS employees
)
SELECT
    registry_code,
    company_name,
    activity,
    trim(BOTH ' ' FROM arrayElement(splitByChar('(', county_raw), 1))                         AS county,
    nullIf(replaceAll(trim(BOTH ' ' FROM arrayElement(splitByChar('(', county_raw), 2)),')',''),'') AS municipality,
    year_val                                                                                   AS year,
    idx                                                                                        AS quarter_num,
    toDate(addMonths(toDate(concat(toString(year_val),'-01-01')), (idx - 1) * 3))             AS quarter_start,
    toDate(addMonths(toDate(concat(toString(year_val),'-01-01')), (idx) * 3) - 1)             AS quarter_end,
    turnover                                                                                   AS turnover_eur,
    state_taxes                                                                                AS state_taxes_eur,
    labour_taxes                                                                               AS labour_taxes_eur,
    employees                                                                                  AS employees_cnt
FROM exploded
WHERE (turnover IS NOT NULL) OR (state_taxes IS NOT NULL) OR (labour_taxes IS NOT NULL) OR (employees IS NOT NULL)
