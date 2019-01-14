with
	cp_activity as (
		select * from {{ ref('cp_activity') }}
	)

	, weeks as (
		select date_trunc('week', date) as week
			, count(*) filter (where is_active)
				/ count(*) :: float as fraction_activities
			, sum(time_spent) filter (where is_active)
				/ sum(time_spent) :: float as fraction_time_spent
		from cp_activity
		where date > '2018-10-01'
		group by 1
	)

select *
from weeks
where fraction_activities < 0.87
	or fraction_time_spent < 0.45
