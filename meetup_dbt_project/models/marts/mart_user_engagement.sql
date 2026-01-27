{{ config(materialized='table') }}

select
    country,
    city,
    count(distinct user_id) as total_users,
    avg(count_group_memberships) as avg_groups_per_user,
    avg(days_to_first_rsvp) as avg_days_to_first_rsvp
from {{ ref('int_users_enriched') }}
group by 1, 2
order by total_users desc