with
	dates as (
		select * from {{ ref('dimension_scorecard_weeks') }}
	)

	, nps as (
		select * from {{ ref('delighted_nps_patient_weekly') }}
	)
	
select *
from dates
left join nps
	using (date_week)
