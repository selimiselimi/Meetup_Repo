{{ config(materialized='table') }}

with venues as (
    select * from {{ ref('stg_venues') }}
),

event_counts as (
    select 
        venue_id, 
        count(distinct event_id) as total_events_hosted,
        max(event_start_at) as last_event_date
    from {{ ref('stg_events') }}
    group by 1
)

select
    v.venue_id,
    v.name,
    v.city,
    v.country,
    v.lat,
    v.lon,
    coalesce(e.total_events_hosted, 0) as total_events_hosted,
    e.last_event_date,
    case 
        when e.total_events_hosted is null or e.total_events_hosted = 0 then 'Unused'
        else 'Active' 
    end as venue_status
from venues v
left join event_counts e on v.venue_id = e.venue_id