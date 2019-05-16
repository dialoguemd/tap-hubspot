with
	outcome_set as (
		select * from {{ ref('careplatform_outcome_set') }}
	)

	, valid_ranked as (
		select episode_id
			, date_day_est
			, outcome
			, row_number() over
				(partition by episode_id, date_day_est order by timestamp)
				as rank
			, row_number() over
				(partition by episode_id, date_day_est order by timestamp desc)
				as rank_reverse
		from outcome_set
		where is_valid_outcome
	)

	, invalid_ranked as (
		select episode_id
			, date_day_est
			, outcome
			, row_number() over
				(partition by episode_id, date_day_est order by timestamp)
				as rank
			, row_number() over
				(partition by episode_id, date_day_est order by timestamp desc)
				as rank_reverse
		from outcome_set
		where is_valid_outcome
	)

select coalesce(valid_ranked.episode_id, invalid_ranked.episode_id)
		as episode_id
	, coalesce(valid_ranked.date_day_est, invalid_ranked.date_day_est)
		as date_day_est
	, coalesce(
		min(valid_ranked.outcome) filter (where valid_ranked.rank = 1)
		, min(invalid_ranked.outcome) filter (where invalid_ranked.rank = 1)
		) as first_outcome
	,  coalesce(
		min(valid_ranked.outcome)
			filter (where valid_ranked.rank_reverse = 1)
		, min(invalid_ranked.outcome)
			filter (where invalid_ranked.rank_reverse = 1)
		) as last_outcome
	, string_agg(invalid_ranked.outcome, ', ' order by invalid_ranked.rank)
		as invalid_outcomes
	, string_agg(valid_ranked.outcome, ', ' order by valid_ranked.rank)
		as valid_outcomes
from valid_ranked
full outer join invalid_ranked
	using (episode_id, date_day_est)
group by 1,2
