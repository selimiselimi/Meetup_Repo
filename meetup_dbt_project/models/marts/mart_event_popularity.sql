{{ config(materialized='table') }}

select
    day_of_week,
    hour_of_day,
    avg(total_rsvp_count) as average_rsvp_count,
    avg(minutes_to_first_rsvp) as avg_minutes_to_first_rsvp,
    avg(capacity_fill_rate) as avg_capacity_fill_rate,
    count(distinct event_id) as total_events_scheduled,
    avg(estimated_attendance) as avg_attendance,
    sum(estimated_attendance) as total_attendance,
    sum(case when is_at_capacity then 1 else 0 end) as count_sold_out_events,
    round(sum(case when is_at_capacity then 1 else 0 end) / count(*) * 100, 2) as pct_sold_out
from {{ ref('int_events_enriched') }}
group by 1, 2
order by avg_attendance desc