with
	templates as (
		select * from {{ ref('careplatform_slash_command_triggered') }}
	)

	, costs as (
		select * from {{ ref('finance_revenue_and_costs_monthly') }}
	)

	, templates_daily as (
		select episode_id
			, date_trunc('day', timestamp) as date_day
			, date_trunc('month', timestamp) as date_month
		from templates
		where command_id = 'Bonjour Sante'
		group by 1,2,3
	)

	, templates_monthly as (
		select date_month
			, count(*)
		from templates_daily
		group by 1
	)

	, cost_monthly as (
		select date_month
			-- For older months set to 0 as the cost was accounted for elsewhere
			, case
				when bonjour_sante_cost <> 0
					then bonjour_sante_cost * 1.0 / count
				else 0
				end as bonjour_sante_cost_per_episode
		from costs
		left join templates_monthly
			using (date_month)
	)

select templates_daily.episode_id
	-- If there's no costs for that month assume it's 7
	, coalesce(
		sum(cost_monthly.bonjour_sante_cost_per_episode)
		, 7) as bonjour_sante_cost
from templates_daily
left join cost_monthly
	using (date_month)
group by 1
