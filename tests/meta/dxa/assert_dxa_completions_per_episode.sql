-- Assert that there is only one completed DXA for at least 98% of episodes
-- started per week

with pivot as (
		select * from analytics_jacob.dxa_question_replies_pivot
	)

	, episodes as (
		select episode_id
			, date_trunc('week', timestamp) as date_week
			, count(*)
		from pivot
		where dxa_completed
		group by 1,2
	)

	, fraction as (
		select date_week
			, count(episode_id) filter (where count = 1) * 1.0
				/ count(episode_id) as unique_fraction
		from episodes
		group by 1
	)

select *
from fraction
where unique_fraction < 0.98
