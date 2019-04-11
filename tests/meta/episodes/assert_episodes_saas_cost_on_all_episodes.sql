with
	episodes as (
		select * from {{ ref('episodes') }}
	)

	, episodes_costs as (
		select * from {{ ref('episodes_costs') }}
	)

select *
from episodes
left join episodes_costs
	using (episode_id)
where episodes.first_message_created_at >= '2018-01-01'
	and episodes.first_set_active is not null
	and episodes_costs.episode_id is null

