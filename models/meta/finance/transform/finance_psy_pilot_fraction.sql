with costs as (
		select * from {{ ref( 'costs_by_episode_daily' ) }}
	)

	, episodes as (
		select * from {{ ref( 'episodes' ) }}
	)

select date_trunc('month', costs.date_day) as date_month
	{% for cost_type in ['cc', 'nc', 'np'] %}
	, case when sum(costs.{{cost_type}}_cost) > 0
		then sum(costs.{{cost_type}}_cost) filter(where episodes.issue_type = 'psy-pilot') *1.0
			/ sum(costs.{{cost_type}}_cost)
		else 0
		end as {{cost_type}}_fraction
	{% endfor %}
	, case when (sum(costs.gp_psy_cost) + sum(costs.gp_other_cost)) > 0
		then (sum(costs.gp_psy_cost) filter(where episodes.issue_type = 'psy-pilot')
			+ sum(costs.gp_other_cost) filter(where episodes.issue_type = 'psy-pilot')) *1.0
			/ (sum(costs.gp_psy_cost) + sum(costs.gp_other_cost))
		else 0
		end as gp_fraction
from costs
left join episodes
    using (episode_id)
where date_day < date_trunc('month', current_timestamp)
group by 1
