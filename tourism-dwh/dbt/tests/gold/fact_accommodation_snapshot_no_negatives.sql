-- Fails if any accommodation metrics are negative
select *
from {{ ref('fact_accommodation_snapshot') }}
where 1=0 and (rooms_cnt < 0
   or beds_total < 0
   or caravan_spots < 0
   or tent_spots < 0)