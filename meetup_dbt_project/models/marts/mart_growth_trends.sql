{{ config(materialized='table') }}

with event_trends as (
    select
        date_trunc('month', event_start_at) as month_year,
        count(distinct event_id) as total_events,
        sum(total_rsvp_count) as total_rsvps,
        sum(estimated_attendance) as total_attendance,
        sum(case when is_at_capacity then 1 else 0 end) as sold_out_events
    from {{ ref('int_events_enriched') }}
    group by 1
),

member_trends as (
    select
        date_trunc('month', portal_joined_at) as month_year,
        count(distinct user_id) as new_members
    from {{ ref('int_users_enriched') }}
    group by 1
),

group_trends as (
    select
        date_trunc('month', group_created_at) as month_year,
        count(distinct group_id) as new_groups
    from {{ ref('int_groups_enriched') }}
    group by 1
),

rsvp_trends as (
    select * from {{ ref('int_rsvp_summary_by_month') }}
),

all_months as (
    select month_year from event_trends
    union
    select month_year from member_trends
    union
    select month_year from group_trends
    union
    select month_year from rsvp_trends
)

select
    a.month_year as report_month,
    coalesce(e.total_events, 0) as total_events,
    coalesce(e.total_rsvps, 0) as total_rsvps,
    coalesce(r.total_guests, 0) as total_guests,
    coalesce(r.waitlist_rsvps, 0) as waitlist_rsvps,
    coalesce(e.total_attendance, 0) as total_attendance,
    coalesce(e.sold_out_events, 0) as sold_out_events,
    coalesce(m.new_members, 0) as new_members,
    coalesce(g.new_groups, 0) as new_groups,
    coalesce(r.monthly_active_members, 0) as monthly_active_members
from all_months a
left join event_trends e on a.month_year = e.month_year
left join member_trends m on a.month_year = m.month_year
left join group_trends g on a.month_year = g.month_year
left join rsvp_trends r on a.month_year = r.month_year
order by 1 desc