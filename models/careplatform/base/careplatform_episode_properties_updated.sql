select episode_id
    , user_id
    , case when properties_issue_type is not null then 'issue_type'
           when properties_outcome is not null then 'outcome'
           else episode_property_type end as episode_property_type
    , coalesce(episode_property_value,
               properties_issue_type,
               properties_outcome)
               as episode_property_value
    , timestamp as updated_at
    , date_trunc('day', timestamp) as date_day
    , date_trunc('day', timezone('America/Montreal', timestamp)) as date_day_est
from careplatform.update_episode_properties
