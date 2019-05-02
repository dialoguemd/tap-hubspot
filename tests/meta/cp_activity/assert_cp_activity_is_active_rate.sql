with
	cp_activity as (
		select * from {{ ref('cp_activity') }}
	)

	, weeks as (
		select date_trunc('week', date) as date_week
			, count(*) filter (where is_active)
				/ count(*) :: float as fraction_activities
			, sum(time_spent) filter (where
					is_active and episode_id is not null
				) / sum(time_spent) filter(where is_active) :: float
			as fraction_time_spent_in_episode
			, sum(time_spent) filter (where
					is_active and shift_location = 'Virtual Care Platform'
				) / sum(time_spent) filter(where
					shift_location = 'Virtual Care Platform'
				) :: float as fraction_active_platform_time_spent
		from cp_activity
		where date > '2018-10-01'
		group by 1
	)

select *
from weeks
-- Calibrated in April 2019
where fraction_activities < 0.95
	or fraction_time_spent_in_episode < 0.83
	or fraction_active_platform_time_spent < 0.66
