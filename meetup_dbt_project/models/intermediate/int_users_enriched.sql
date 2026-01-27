{{ config(materialized='table') }}

with user_memberships as (
    select * from {{ ref('stg_users') }}
),

user_stats as (
    select
        user_id,
        max(city) as city,
        max(country) as country,
        min(joined) as portal_joined_at,
        count(distinct group_id) as count_group_memberships
    from user_memberships
    group by 1
),

first_rsvp as (
    select
        user_id,
        min(rsvp_when) as first_rsvp_at
    from {{ ref('stg_rsvp') }}
    group by 1
)

select
    u.user_id,
    u.city,
    u.country,
    u.portal_joined_at,
    u.count_group_memberships,
    r.first_rsvp_at,
    datediff('day', u.portal_joined_at, r.first_rsvp_at) as days_to_first_rsvp
from user_stats u
left join first_rsvp r on u.user_id = r.user_id