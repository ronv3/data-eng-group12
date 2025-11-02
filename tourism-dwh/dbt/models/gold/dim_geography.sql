{{ config(
  schema='gold',
  materialized='table',
  engine='MergeTree()',
  order_by=['geo_sk']
) }}

with src as (
  select
      lowerUTF8(nullIf(replaceRegexpAll(toString(region), '^[[:space:]]+|[[:space:]]+$', ''), ''))  as region_norm,
      lowerUTF8(nullIf(replaceRegexpAll(toString(island), '^[[:space:]]+|[[:space:]]+$', ''), ''))  as island_norm
  from {{ ref('stg_housing_accommodation') }}
)
select
    xxHash64(concat(coalesce(region_norm, ''), '|', coalesce(island_norm, ''))) as geo_sk,
    coalesce(region_norm, '') as region,
    coalesce(island_norm, '') as island
from src
group by region, island