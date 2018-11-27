with
	dates as (
		select * from {{ ref('dimension_scorecard_weeks') }}
	)

	, opportunities as (
		select * from {{ ref('salesforce_opportunities_won_weekly') }}
	)
	
select date_trunc('quarter', dates.date_week) as date_quarter
	, dates.date_week
	, opportunities.weekly_closed_mrr
	, opportunities.weekly_closed_mrr_roc
	, opportunities.weekly_closed_mrr_partner_lead
	, opportunities.weekly_closed_mrr_partner_influenced
	, opportunities.closed_mrr
	, opportunities.closed_mrr_roc
	, opportunities.closed_mrr_partner_lead
	, opportunities.closed_mrr_partner_influenced
-- TODO replace with a seed for Q1-19 targets
	, (extract(week from dates.date_week) - 39) * 11860
		as "Target MRR"
	, (extract(week from dates.date_week) - 39) * 3846
		as "Target MRR ROC"
	, (extract(week from dates.date_week) - 39) * 2724
		as "Target MRR - Partnership"
from dates
left join opportunities
	on dates.date_week = opportunities.date_week
		and opportunities.date_week < date_trunc('week', current_date)
