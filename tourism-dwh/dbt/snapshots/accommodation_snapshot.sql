{% snapshot accommodation_snapshot %}
{{ config(
    target_schema='gold',
    unique_key='accommodation_bk',
    strategy='check',
    check_cols=['name','category','type','stars','booking_link','socials','seasonal_flag'],
    invalidate_hard_deletes=true
) }}

-- Build a stable business key: property_bk + name
select
  concat(toString(property_bk), '|', coalesce(name, '')) as accommodation_bk,
  property_bk,
  name,
  category,
  type,
  stars,
  booking_link,
  socials,
  seasonal_flag
from {{ ref('stg_housing_accommodation') }}

{% endsnapshot %}
