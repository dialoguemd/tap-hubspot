with
	revenue_monthly as (
		select * from {{ ref('finance_revenue_and_costs_monthly') }}
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

	, saas_costs_monthly as (
		select date_month_est as date_month
			, round(sum(saas_cost)) as saas_cost
		from episodes
		group by 1
	)

select *
from revenue_monthly
left join saas_costs_monthly
	using (date_month, saas_cost)
where revenue_monthly.date_month >= '2018-01-01'
	and saas_costs_monthly.date_month is null
