{{ config(materialized='table') }}

with groups as (
    select * from {{ ref('int_groups_enriched') }}
),

exploded as (
    select
        group_id,
        group_name,
        city,
        total_members,
        total_events_hosted,
        regexp_replace(value::string,'"','') as topic_name
    from groups,
    lateral flatten(input => topics)
)

select * from exploded
where length(topic_name) > 0