{{ config(materialized='table') }}

select
    venue_id,
    venue_name,
    venue_city,
    venue_country,
    count(distinct event_id) as events_hosted,
    sum(estimated_attendance) as total_foot_traffic,
    avg(estimated_attendance) as avg_event_size
from {{ ref('int_events_enriched') }}
where venue_id is not null
group by 1, 2, 3, 4
order by events_hosted desc