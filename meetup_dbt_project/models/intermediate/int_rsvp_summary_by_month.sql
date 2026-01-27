{{ config(materialized='table') }}

select
    date_trunc('month', rsvp_when) as month_year,
    count(distinct case when rsvp_response = 'yes' then user_id end) as monthly_active_members,
    sum(rsvp_guests) as total_guests,
    count(case when rsvp_response = 'waitlist' then 1 end) as waitlist_rsvps
from {{ ref('stg_rsvp') }}
group by 1