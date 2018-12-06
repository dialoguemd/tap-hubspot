with
	opportunities as (
		select * from {{ ref('salesforce_opportunities_detailed') }}
	)

	, funnelio as (
		select * from {{ ref('funnelio_performance_metrics') }}
	)

	, meetings_monthly as (
		select date_trunc('month', meeting_date) as date_month
			, count(*) as meetings
		from opportunities
		where is_inbound
		group by 1
	)

	, costs_monthly as (
		select date_month
		    , sum(cost) as cost
		from funnelio
		group by 1
	)

select costs_monthly.date_month
	, costs_monthly.cost
	, coalesce(meetings_monthly.meetings, 0) as meetings
from costs_monthly
left join meetings_monthly
	using (date_month)
