with
	dates as (
		select * from {{ ref('dimension_scorecard_weeks') }}
	)

	, opportunities as (
		select * from {{ ref('salesforce_opportunities_won_weekly') }}
	)
	
select *
-- TODO replace with a seed for Q1-19 targets
	, (extract(week from dates.date_week) - 39) * 11860
		as "Target MRR"
	, (extract(week from dates.date_week) - 39) * 3846
		as "Target MRR ROC"
	, (extract(week from dates.date_week) - 39) * 2724
		as "Target MRR - Partnership"
from dates
left join opportunities
	using (date_week)
