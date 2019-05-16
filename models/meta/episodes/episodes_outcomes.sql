with
	careplatform_outcome_set as (
		select * from {{ ref('careplatform_outcome_set') }}
	)

	, outcome_rank_valid as (
		select episode_id
			, outcome
			, outcome_category
			, timestamp
			, last_value(outcome) over(
				partition by episode_id
				order by timestamp desc
				rows between unbounded preceding and unbounded following
			) as current_outcome
			, last_value(outcome_category) over(
				partition by episode_id
				order by timestamp desc
				rows between unbounded preceding and unbounded following
			) as current_outcome_category
			, row_number() over(
				partition by episode_id
				order by timestamp desc
			) as rank
		from careplatform_outcome_set
		where is_valid_outcome
	)

	, outcome_rank_all as (
		select episode_id
			, outcome
			, outcome_category
			, timestamp
			, first_value(outcome) over(
				partition by episode_id
				order by timestamp
				rows between unbounded preceding and unbounded following
			) as first_outcome
			, first_value(outcome_category) over(
				partition by episode_id
				order by timestamp
				rows between unbounded preceding and unbounded following
			) as first_outcome_category
			, last_value(outcome) over(
				partition by episode_id
				order by timestamp
				rows between unbounded preceding and unbounded following
			) as current_outcome
			, last_value(outcome_category) over(
				partition by episode_id
				order by timestamp
				rows between unbounded preceding and unbounded following
			) as current_outcome_category
			, row_number() over(
				partition by episode_id
				order by timestamp desc
			) as rank
		from careplatform_outcome_set
	)

	, episode_outcome as (
		select outcome_rank_all.episode_id
			, outcome_rank_all.first_outcome
			, outcome_rank_all.first_outcome_category
			, coalesce(outcome_rank_valid.current_outcome
				, outcome_rank_all.current_outcome
			) as outcome
			, coalesce(outcome_rank_valid.current_outcome_category
				, outcome_rank_all.current_outcome_category
			) as outcome_category
			, outcome_rank_valid.current_outcome_category is not null
				as is_valid_outcome
			, array_agg(
				outcome_rank_all.outcome
				order by outcome_rank_all.timestamp asc
			) as outcomes_ordered
			, string_agg(
				outcome_rank_all.outcome
				, ', '
				order by outcome_rank_all.timestamp asc
			) as outcomes_ordered_str
			, min(outcome_rank_all.timestamp) as timestamp
		from outcome_rank_all
		left join outcome_rank_valid
			on outcome_rank_all.episode_id = outcome_rank_valid.episode_id
			and outcome_rank_valid.rank = 1
		{{ dbt_utils.group_by(6) }}
	)

select episode_id
	, first_outcome_category
	, first_outcome
	, outcome_category
	, outcome
	, is_valid_outcome
	, outcomes_ordered
	, timestamp as outcome_first_set_timestamp
from episode_outcome
