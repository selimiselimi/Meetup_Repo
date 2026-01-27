{{ config(materialized='table') }}

select
    regexp_replace(f.value:city::string, '"', '')                    as city,
    TO_TIMESTAMP_NTZ(TO_NUMBER(f.value:created) / 1000)              as created,
    regexp_replace(f.value:description::string, '<[^>]+>', '')       as description,
    regexp_replace(f.value:name::string, '"', '')                    as name,
    f.value:lat::float                                               as lat,
    f.value:lon::float                                               as lon,
    regexp_replace(f.value:link::string, '"', '')                    as link,
    regexp_replace(f.value:group_id::string, '"', '')                as group_id,
    f.value:topics                                                   as topics
from {{ source('raw_data', 'groups') }} r,
     lateral flatten(input => r.data) f