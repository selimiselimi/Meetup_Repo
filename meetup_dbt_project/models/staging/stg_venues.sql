{{ config(materialized='table') }}

select distinct
    regexp_replace(v.value:venue_id::string, '"', '')     as venue_id,
    regexp_replace(v.value:name::string, '"', '')         as name,
    regexp_replace(v.value:city::string, '"', '')         as city,
    regexp_replace(v.value:country::string, '"', '')      as country,
    v.value:lat::float                                    as lat,
    v.value:lon::float                                    as lon
from {{ source('raw_data', 'venues') }} r,
     lateral flatten(input => r.data) v
