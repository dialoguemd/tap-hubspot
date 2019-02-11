with costs as (
		select * from {{ ref( 'costs_by_episode_daily' ) }}
	)

	, episodes as (
		select * from {{ ref( 'episodes' ) }}
	)

select date_trunc('month', costs.date_day) as date_month
	, sum(costs.cc_cost) filter(where episodes.issue_type = 'psy-pilot') *1.0
		/ sum(costs.cc_cost) as cc_fraction
	, sum(costs.nc_cost) filter(where episodes.issue_type = 'psy-pilot') *1.0
		/ sum(costs.nc_cost) as nc_fraction
	, sum(costs.np_cost) filter(where episodes.issue_type = 'psy-pilot') *1.0
		/ sum(costs.np_cost) as np_fraction
	, (sum(costs.gp_psy_cost) filter(where episodes.issue_type = 'psy-pilot')
		+ sum(costs.gp_other_cost) filter(where episodes.issue_type = 'psy-pilot')) *1.0
		/ (sum(costs.gp_psy_cost) + sum(costs.gp_other_cost)) as gp_fraction
from costs
left join episodes
    using (episode_id)
where date_day < date_trunc('month', current_timestamp)
group by 1
