{{ config(materialized='table') }}

select
    group_id,
    group_name,
    group_created_at,
    city as group_city,
    topics,
    total_members,
    total_events_hosted,
    last_event_date,
    days_since_last_event,
    case 
        when days_since_last_event > 180 then 'Inactive'
        else 'Active'
    end as activity_status
from {{ ref('int_groups_enriched') }}
order by total_members desc