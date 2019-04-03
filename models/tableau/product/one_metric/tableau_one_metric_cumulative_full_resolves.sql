with episodes as (
        select * from {{ ref('tableau_product_one_metric_resolution_rate') }}
    )

	, resolves as (
		select date_trunc('week', episode_started_at) as week
			, count(episode_id) as count_episodes
			, count(episode_id) filter
				(where fully_resolved) as count_fully_resolved
			, count(episode_id) filter
				(where outcome is null and episode_resolved_at is not null)
				as count_resolved_no_outcome
			, count(episode_id)
				filter (where episode_resolved_at is null) as count_no_resolve
		from episodes
		group by 1
	)

	, full_resolves as (
		select week
			, round( 
				case
					-- For episodes created prior to 2017-12-01, which is when resolves were
					-- introduced, assume that 90% were resolved

					-- Of episodes resolved without an outcome, assume conservatively that
					-- 25% are full resolves 

					when week < '2017-12-01'
						then (count_no_resolve * 0.9 * 0.25)
							+ (count_resolved_no_outcome * 0.25)
							+ count_fully_resolved

					-- Use 25% only until new outcomes were added in 2018-03-01

					when week < '2018-03-01'
						then (count_resolved_no_outcome * 0.25)
						+ count_fully_resolved

					else count_fully_resolved
					end
				) as count_fully_resolved
			, count_episodes
		from resolves
	)

select week
	, count_fully_resolved
	, count_episodes
	, count_fully_resolved / count_episodes :: float as full_resolve_rate
from full_resolves
