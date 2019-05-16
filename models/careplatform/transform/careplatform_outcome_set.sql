with
	properties_updated as (
		select * from {{ ref('careplatform_episode_properties_updated') }}
	)

	, dimension_outcome as (
		select * from {{ ref('careplatform_dimension_outcome') }}
	)

select properties_updated.episode_id
	, properties_updated.timestamp
	, {{ expand_date('properties_updated') }}
	, {{ to_est('properties_updated') }}
	, {{ expand_date_est('properties_updated') }}
	, properties_updated.user_id
	, dimension_outcome.outcome
	, dimension_outcome.outcome_category
	, dimension_outcome.is_valid_outcome
from properties_updated
left join dimension_outcome
	on properties_updated.episode_property_value
		= dimension_outcome.outcome_raw
where properties_updated.episode_property_type = 'outcome'
	and properties_updated.episode_id is not null
	and properties_updated.user_id is not null
