with daily_time_spent_by_ep as (
    select * from {{ ref( 'costs_time_spent_by_episode_daily' ) }}
    )

    , fl_costs as (
        select * from {{ ref( 'finance_revenue_and_costs_monthly' ) }}
    )

    , monthly_activities as (
        select date_trunc('month', date) as date_month
            , sum(cc_time) as cc_time
            , sum(nc_time) as nc_time
            , sum(np_time) as np_time
        from daily_time_spent_by_ep
        group by 1
    )

    select date_month
        , coalesce(fl_costs.fl_cc_cost*1.0
            / monthly_activities.cc_time, 0) as cc_hourly
        , coalesce(fl_costs.fl_nc_cost*1.0
            / monthly_activities.nc_time, 0) as nc_hourly
        , coalesce(fl_costs.fl_np_cost*1.0
            / monthly_activities.np_time, 0) as np_hourly
    from monthly_activities
    left join fl_costs using (date_month)
