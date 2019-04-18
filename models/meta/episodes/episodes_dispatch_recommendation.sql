with
	slash_command_triggered as (
		select * from {{ ref('careplatform_slash_command_triggered') }}
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

select episode_id
	, dispatch_recommendation
	, dispatch_recommendation_timestamp
	, dispatch_recommendation_timestamp_est
from ranked
where rank = 1
