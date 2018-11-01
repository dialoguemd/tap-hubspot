with daily_time_spent_by_ep as (
    select * from {{ ref( 'medops_daily_time_spent_by_ep' ) }}
    )

    , fl_costs as (
        select * from {{ ref( 'medops_fl_costs_by_main_spec' ) }}
    )

    , monthly_activities as (
        select date_trunc('month', date) as month
            , sum(cc_time) as cc_time
            , sum(nc_time) as nc_time
            , sum(np_time) as np_time
        from daily_time_spent_by_ep
        group by 1
    )
    
    select month
        , fl_cc_cost*1.0 / cc_time as cc_hourly
        , fl_nc_cost*1.0 / nc_time as nc_hourly
        , fl_np_cost*1.0 / np_time as np_hourly
    from monthly_activities
    left join fl_costs using (month)
