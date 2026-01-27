{{ config(materialized='table') }}

with base as (
    select
        regexp_replace(e.value:group_id::string, '"', '') as group_id,
        regexp_replace(r.value:user_id::string, '"', '') as user_id,
        regexp_replace(e.value:name::string, '"', '') as event_name,
        to_timestamp_ntz(to_number(e.value:created) / 1000) as event_created_at,
        to_timestamp_ntz(to_number(e.value:time) / 1000) as event_start_at,
        regexp_replace(e.value:venue_id::string, '"', '') as venue_id,
        to_timestamp_ntz(to_number(r.value:when) / 1000) as rsvp_when,
        regexp_replace(r.value:response::string, '"', '') as rsvp_response,
        r.value:guests::number as rsvp_guests
    from {{ source('raw_data', 'events') }} r0,
         lateral flatten(input => r0.data) e,
         lateral flatten(input => e.value:rsvps) r
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