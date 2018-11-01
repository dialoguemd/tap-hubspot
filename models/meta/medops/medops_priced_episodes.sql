with daily_time_spent_by_ep as (
        select * from {{ ref( 'medops_daily_time_spent_by_ep' ) }}
    )

    , monthly_hourly_cost_by_spec as (
        select * from {{ ref( 'medops_hourly_cost_by_spec_monthly' ) }}
    )

	select daily_time_spent_by_ep.episode_id
	    , daily_time_spent_by_ep.date
	    , cc_time * cc_hourly as cc_cost
	    , nc_time * nc_hourly as nc_cost
	    , np_time * np_hourly as np_cost
	from daily_time_spent_by_ep
	left join monthly_hourly_cost_by_spec
		on date_trunc('month', daily_time_spent_by_ep.date) = monthly_hourly_cost_by_spec.month
