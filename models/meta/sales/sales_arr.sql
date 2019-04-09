with
	months as (
		select * from {{ ref('dimension_months') }}
	)

	, opportunities as (
		select * from {{ ref('salesforce_opportunities_detailed') }}
	)
	
	, revenue_monthly as (
		select * from {{ ref('finance_revenue_monthly') }}
	)

	, backlog as (
		select months.date_month
			, sum(opportunities.amount) as mrr_backlog
			, sum(opportunities.amount) filter(where
				opportunities.close_date >= months.date_month
			) as mrr_backlog_new
			, sum(opportunities.amount) filter(where
				opportunities.close_date < months.date_month
			) as mrr_backlog_old
		from months
		left join opportunities
			on months.month_end > opportunities.close_date
				and months.month_end <= opportunities.launch_date
		where is_won
		group by 1
	)

select revenue_monthly.date_month
	, revenue_monthly.mrr
	, backlog.mrr_backlog
	, backlog.mrr_backlog_new
	, backlog.mrr_backlog_old
from revenue_monthly
left join backlog
	using (date_month)
