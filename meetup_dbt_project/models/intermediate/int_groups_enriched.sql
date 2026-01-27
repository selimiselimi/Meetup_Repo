{{ config(materialized='table') }}

with groups as (
    select * from {{ ref('stg_groups') }}
),
membership_counts as (
    select
        group_id,
        count(distinct user_id) as total_members
    from {{ ref('stg_users') }}
    group by 1
),
event_stats as (
    select
        group_id,
        count(distinct event_id) as total_events_hosted,
        max(event_start_at) as last_event_date
    from {{ ref('stg_events') }}
    group by 1
)

select
    g.group_id,
    g.name as group_name,
    g.city,
    g.topics,
    g.created as group_created_at,
    coalesce(m.total_members, 0) as total_members,
    coalesce(e.total_events_hosted, 0) as total_events_hosted,
    e.last_event_date,
    datediff('day', e.last_event_date, current_date) as days_since_last_event
from groups g
left join membership_counts m on g.group_id = m.group_id
left join event_stats e on g.group_id = e.group_id