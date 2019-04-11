{% set roles = ['cc', 'nc', 'np'] %}

with
	daily_time_spent_by_ep as (
		select * from {{ ref('costs_time_spent_by_episode_daily') }}
	)

	, fl_costs as (
		select * from {{ ref('finance_revenue_and_costs_monthly') }}
	)

	, monthly_activities as (
		select date_trunc('month', date) as date_month

{% for role in roles %}

			, sum({{role}}_time) as {{role}}_time

{% endfor %}

		from daily_time_spent_by_ep
		group by 1
	)

select date_month

{% for role in roles %}

	, coalesce(
		fl_costs.fl_{{role}}_cost * 1.0
		/ monthly_activities.{{role}}_time
		, 0
	) as {{role}}_hourly
	, coalesce(
		(
			fl_costs.fl_{{role}}_cost
			+ fl_costs.adjustments_{{role}}
{% if role == 'nc' %}
			+ fl_costs.licenses_cost
{% endif %}
		) * 1.0
		/ monthly_activities.{{role}}_time
		, 0
	) as {{role}}_hourly_ops
{% endfor %}

from monthly_activities
inner join fl_costs
	using (date_month)
