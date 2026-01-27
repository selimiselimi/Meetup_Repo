{{ config(materialized='table') }}

select
    regexp_replace(u.value:user_id::string, '"', '')     as user_id,
    regexp_replace(u.value:city::string, '"', '')        as city,
    regexp_replace(u.value:country::string, '"', '')     as country,
    regexp_replace(u.value:hometown::string, '"', '')    as hometown,
    TO_TIMESTAMP_NTZ(TO_NUMBER(m.value:joined) / 1000)   as joined,
    regexp_replace(m.value:group_id::string, '"', '')    as group_id
from {{ source('raw_data', 'users') }} r,
     lateral flatten(input => r.data) u,
     lateral flatten(input => u.value:memberships) m
