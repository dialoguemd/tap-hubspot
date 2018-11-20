with
	gm as (
		select * from {{ ref('medops_gm1_weekly')}}
	)

	, dates as (
		select * from {{ ref('dimension_scorecard_weeks')}}
	)

select *
from dates
left join gm
	using (date_week)
