select
    topic_name,
    count(distinct group_id) as total_groups,
    sum(total_members) as potential_member_reach,
    sum(total_events_hosted) as total_events
from {{ ref('int_group_topics') }}
group by 1
order by 3 desc