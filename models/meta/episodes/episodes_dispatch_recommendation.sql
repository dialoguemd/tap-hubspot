with
	slash_command_triggered as (
		select * from {{ ref('careplatform_slash_command_triggered') }}
	)

	, dim_dispatch_recommendation as (
		select * from {{ ref('dimension_dispatch_recommendation') }}
	)

	, ranked as (
		select episode_id
			, timestamp as dispatch_recommendation_timestamp
			, timestamp_est as dispatch_recommendation_timestamp_est
			, command_id as dispatch_recommendation
			, rank() over(partition by episode_id order by timestamp) as rank
		from slash_command_triggered
		where command_id like 'Outcome%'
	)

select ranked.episode_id
	, dim_dispatch_recommendation.dispatch_recommendation
	, ranked.dispatch_recommendation_timestamp
	, ranked.dispatch_recommendation_timestamp_est
from ranked
left join dim_dispatch_recommendation
	on ranked.dispatch_recommendation
		= dim_dispatch_recommendation.dispatch_recommendation_raw
where ranked.rank = 1
