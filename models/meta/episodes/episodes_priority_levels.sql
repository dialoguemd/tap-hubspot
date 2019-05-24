with careplatform_episode_properties_updated as (
        select * from {{ ref('careplatform_episode_properties_updated') }}
    )

    , priority_rank as (
        select episode_id
            , episode_property_value as priority_level
            , updated_at
            , first_value(episode_property_value) over(partition by episode_id order by updated_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first
            , last_value(episode_property_value) over(partition by episode_id order by updated_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as current
        from careplatform_episode_properties_updated
        where episode_property_type = 'priority_level'
            and episode_id is not null
    )

    , episode_priority as (
        select episode_id
            , min(updated_at) as updated_at
            , first as first_priority_level
            , current as priority_level
            , array_agg(priority_level order by updated_at asc) as priority_levels_ordered
        from priority_rank
        group by 1,3,4
    )

select episode_id
    , first_priority_level
    , priority_level
    , priority_levels_ordered
    , updated_at as priority_first_set_timestamp
from episode_priority
