{% snapshot company_snapshot %}
{{ config(
    target_schema='gold',
    unique_key='registry_code',
    strategy='check',
    check_cols=['name','activity','website','email','phone','address','municipality','county'],
    invalidate_hard_deletes=true
) }}

select
  registry_code,
  name,
  activity,
  website,
  email,
  phone,
  address,
  municipality,
  county
from {{ ref('stg_company_latest') }}

{% endsnapshot %}
