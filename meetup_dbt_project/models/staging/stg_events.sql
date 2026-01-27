{{ config(materialized='table') }}

with base as (
    select distinct
        regexp_replace(e.value:group_id::string, '"', '') as group_id,
        regexp_replace(e.value:name::string, '"', '') as event_name,
        regexp_replace(e.value:description::string, '<[^>]+>', '') as event_description,
        to_timestamp_ntz(to_number(e.value:created) / 1000) as event_created_at,
        to_timestamp_ntz(to_number(e.value:time) / 1000) as event_start_at,
        e.value:duration::number as duration_seconds,
        e.value:rsvp_limit::number as rsvp_limit,
        regexp_replace(e.value:venue_id::string, '"', '') as venue_id,
        regexp_replace(e.value:status::string, '"', '') as status
    from {{ source('raw_data', 'events') }} r0,
         lateral flatten(input => r0.data) e
)

select
    base.*,
    {{ meetup_surrogate_key([
        'group_id',
        "lower(trim(event_name))",
        'event_start_at',
        'venue_id',
        'event_created_at'
    ]) }} as event_id
from base
where event_start_at is not null
