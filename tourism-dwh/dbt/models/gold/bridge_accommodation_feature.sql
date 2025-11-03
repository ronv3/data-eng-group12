{{ config(
  schema='gold',
  materialized='table',
  engine='MergeTree()',
  order_by=['accommodation_sk','feature_sk']
) }}

with f as (
  select
      lowerUTF8(toString(property_bk))  as property_bk_norm,
      lowerUTF8(toString(feature_name)) as feature_name_norm
  from {{ ref('stg_housing_features') }}
  where property_bk is not null
    and feature_name is not null
),
acc_current as (
  select
      accommodation_sk,
      lowerUTF8(toString(property_bk_norm)) as property_bk_norm
  from {{ ref('dim_accommodation') }}
  where is_current = 1
),
feat as (
  select
      feature_sk,
      lowerUTF8(toString(name)) as feature_name_norm
  from {{ ref('dim_feature') }}
)
select
    assumeNotNull(acc_current.accommodation_sk) as accommodation_sk,
    assumeNotNull(feat.feature_sk)              as feature_sk
from f
join acc_current using (property_bk_norm)
join feat       using (feature_name_norm)
group by accommodation_sk, feature_sk