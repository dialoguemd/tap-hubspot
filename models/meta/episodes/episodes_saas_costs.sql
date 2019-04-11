with
	chats_summary as (
		select * from {{ ref('episodes_chats_summary') }}
	)

	, costs as (
		select * from {{ ref('finance_revenue_and_costs_monthly') }}
	)

	, episodes_monthly as (
		select date_month_est as date_month
			, episode_id
			, count(*) over (partition by date_month_est)
				as episode_count_monthly
		from chats_summary
		where first_set_active is not null
	)

select episodes_monthly.episode_id
	, coalesce(
		costs.saas_cost / episodes_monthly.episode_count_monthly,
		.5
	) as saas_cost
from episodes_monthly
left join costs
	using (date_month)
where episodes_monthly.date_month >= '2018-01-01'
