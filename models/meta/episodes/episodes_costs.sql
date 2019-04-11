{% set roles = ['nc', 'np', 'cc', 'gp', 'total'] %}
{% set days_lag = [3, 7, 14, 28] %}

with
	chats_summary as (
		select * from {{ ref('episodes_chats_summary') }}
	)

	, daily_costs as (
		select * from {{ ref('costs_by_episode_daily') }}
	)

	, saas_costs as (
		select * from {{ ref('episodes_saas_costs') }}
	)

	, staff_costs as (
		select chats_summary.episode_id

		{% for role in roles %}

			, sum(daily_costs.{{role}}_cost) as {{role}}_cost_total
			, sum(daily_costs.{{role}}_cost_ops) as {{role}}_cost_ops_total

			{% for days in days_lag %}

			, sum(daily_costs.{{role}}_cost)
				filter(where
					daily_costs.date_day < chats_summary.date_day + interval '{{days}} days'
				) as {{role}}_cost_{{days}}_days
			, sum(daily_costs.{{role}}_cost_ops)
				filter(where
					daily_costs.date_day < chats_summary.date_day + interval '{{days}} days'
				) as {{role}}_cost_ops_{{days}}_days

			{% endfor %}

		{% endfor %}

		from chats_summary
		inner join daily_costs
			using (episode_id)
		group by 1
	)

select coalesce(staff_costs.episode_id, saas_costs.episode_id) as episode_id

	{% for role in roles %}
		{% if role != 'total' %}

	, coalesce(staff_costs.{{role}}_cost_total, 0) as {{role}}_cost_total
	, coalesce(staff_costs.{{role}}_cost_ops_total, 0)
		as {{role}}_cost_ops_total

			{% for days in days_lag %}

	, coalesce(staff_costs.{{role}}_cost_{{days}}_days, 0)
		as {{role}}_cost_{{days}}_days
	, coalesce(staff_costs.{{role}}_cost_ops_{{days}}_days, 0)
		as {{role}}_cost_ops_{{days}}_days

			{% endfor %}
		{% endif %}

	{% endfor %}

	, coalesce(staff_costs.total_cost_total, 0)
		+ coalesce(saas_costs.saas_cost, 0)
		as total_cost_total
	, coalesce(staff_costs.total_cost_ops_total, 0)
		+ coalesce(saas_costs.saas_cost, 0)
		as total_cost_ops_total

	{% for days in days_lag %}

	, coalesce(total_cost_{{days}}_days, 0)
		+ coalesce(saas_costs.saas_cost, 0)
		as total_cost_{{days}}_days
	, coalesce(total_cost_ops_{{days}}_days, 0)
		+ coalesce(saas_costs.saas_cost, 0)
		as total_cost_ops_{{days}}_days

	{% endfor %}

	, saas_costs.saas_cost
from staff_costs
full join saas_costs
	using (episode_id)
