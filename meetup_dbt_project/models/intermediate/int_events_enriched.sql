{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_events') }}
),

venues as (
    select * from {{ ref('stg_venues') }}
),

rsvp_metrics as (
    select
        event_id,
        count(distinct user_id) as total_rsvp_count,
        sum(rsvp_guests) as total_guests_brought,
        min(rsvp_when) as first_rsvp_timestamp,
        count(distinct user_id) + sum(coalesce(rsvp_guests, 0)) as total_attendance
    from {{ ref('stg_rsvp') }}
    where rsvp_response = 'yes'
    group by 1
)

select
    e.event_id,
    e.event_name,
    e.group_id,
    e.event_start_at,
    e.event_created_at,
    e.rsvp_limit,
    v.venue_id,
    v.name as venue_name,
    v.city as venue_city,
    v.country as venue_country,
    dayname(e.event_start_at) as day_of_week,
    hour(e.event_start_at) as hour_of_day,
    coalesce(r.total_rsvp_count, 0) as total_rsvp_count,
    coalesce(r.total_attendance, 0) as estimated_attendance,
    datediff('minute', e.event_created_at, r.first_rsvp_timestamp) as minutes_to_first_rsvp,
    case 
        when e.rsvp_limit > 0 and r.total_rsvp_count >= e.rsvp_limit then true 
        else false 
    end as is_at_capacity,
    case 
        when e.rsvp_limit > 0 then (r.total_rsvp_count / e.rsvp_limit) * 100
        else null 
    end as capacity_fill_rate
from events e
left join venues v on e.venue_id = v.venue_id
left join rsvp_metrics r on e.event_id = r.event_id