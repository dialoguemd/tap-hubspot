with
	user_contract as (
		select * from {{ ref('user_contract') }}
	)

	, wiw_costs_daily as (
		select * from {{ ref('wiw_costs_daily') }}
	)

	, revenue_yesterday as (
		select sum(
			case
				when charge_strategy  = 'dynamic'
				then charge_price
				else 9
			end) / 30 as revenue
		from user_contract
		where is_employee
			and current_date - interval '1 day' <@ during_est
			and charge_price <> 0
	)

select (revenue_yesterday.revenue - wiw_costs_daily.virtual_costs_fl)
		/ revenue_yesterday.revenue as gm1
from revenue_yesterday
inner join wiw_costs_daily
	on true
where wiw_costs_daily.start_date = current_date - interval '1 day'
