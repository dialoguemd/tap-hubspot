with properties_updated as (
        select * from {{ ref('careplatform_episode_properties_updated') }}
    )

select episode_id
	, episode_property_value as outcome
	, timestamp
	, user_id
from properties_updated
where episode_property_type = 'outcome'
	and episode_id is not null
	and user_id is not null