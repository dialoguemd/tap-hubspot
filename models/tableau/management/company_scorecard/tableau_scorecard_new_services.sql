with
	dates as (
		select * from {{ ref('dimension_scorecard_weeks') }}
	)

	, new_services as (
		select * from {{ ref('salesforce_new_services_won_weekly') }}
	)

	, new_services_cum as (
		select date_quarter
			, date_week
			, mrr_stress_mgmt
			, mrr_24_7
			, sum(mrr_stress_mgmt) over(
				partition by date_quarter order by date_week
			) as mrr_stress_mgmt_cum
			, sum(mrr_24_7) over(
				partition by date_quarter order by date_week
			) as mrr_24_7_cum
		from new_services
		group by 1,2,3,4
	)

select date_trunc('quarter', dates.date_week) as date_quarter
	, dates.date_week
	, new_services_cum.mrr_stress_mgmt
	, new_services_cum.mrr_24_7
	, new_services_cum.mrr_stress_mgmt_cum
	, new_services_cum.mrr_24_7_cum
from dates
left join new_services_cum
	on dates.date_week = new_services_cum.date_week
		and new_services_cum.date_week < date_trunc('week', current_date)
