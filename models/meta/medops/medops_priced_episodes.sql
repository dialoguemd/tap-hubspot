with time_spent as (
        select * from {{ ref( 'medops_daily_time_spent_by_ep' ) }}
    )

    , hourly_cost as (
        select * from {{ ref( 'medops_hourly_cost_by_spec_monthly' ) }}
    )

    , episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

select time_spent.episode_id
    , time_spent.date
    , episodes.user_id
    , time_spent.cc_time * hourly_cost.cc_hourly as cc_cost
    , time_spent.nc_time * hourly_cost.nc_hourly as nc_cost
    , time_spent.np_time * hourly_cost.np_hourly as np_cost
from time_spent
left join episodes using (episode_id)
left join hourly_cost
	on date_trunc('month', time_spent.date)
        = hourly_cost.date_month
