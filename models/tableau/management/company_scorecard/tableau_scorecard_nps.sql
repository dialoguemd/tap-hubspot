with
	dates as (
		select * from {{ ref('dimension_scorecard_weeks') }}
	)

	, nps as (
		select * from {{ ref('delighted_nps_patient_weekly') }}
	)
	
select dates.date_week
	, nps.nps
	, nps.promoter_count
	, nps.passive_count
	, nps.detractor_count
	, nps.respondent_count
from dates
left join nps
	on dates.date_week = nps.date_week
		and nps.date_week < date_trunc('week', current_date)
