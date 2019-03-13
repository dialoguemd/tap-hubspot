{% set cost_types = ["virtual_costs_fl", "virtual_video_costs_fl",
		"virtual_chat_costs_fl", "virtual_costs_gp_fl", "virtual_costs_np_fl",
		"virtual_costs_nc_fl", "virtual_costs_cc_fl"] %}
with
	costs_daily as (
		select * from {{ ref('wiw_costs_daily') }}
	)

select date_trunc('week', start_date) as start_week

	{% for cost_type in cost_types %}

	, sum({{cost_type}}) as {{cost_type}}

	{% endfor %}
from costs_daily
group by 1
