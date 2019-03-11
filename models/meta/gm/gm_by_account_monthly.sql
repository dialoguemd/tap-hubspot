with
    user_contract as (
        select * from {{ ref ( 'user_contract' ) }}
    )

    , daily_costs as (
        select * from {{ ref ( 'costs_by_episode_daily' ) }}
    )

    , revenue_tmp as (
        select * from {{ ref ( 'finance_adjusted_revenue_monthly' ) }}
    )

    , months_tmp as (
        select * from {{ ref ( 'dimension_months' ) }}
    )

    , months as (
        select date_month
            , tsrange(months_tmp.date_month::timestamp,
                months_tmp.date_month::timestamp + interval '1 month')
                as month_range_est
        from months_tmp
    )

    , costs as (
        select months.date_month
            , user_contract.account_id
            , user_contract.account_name
            , sum(daily_costs.cc_cost) as cc_cost
            , sum(daily_costs.nc_cost) as nc_cost
            , sum(daily_costs.np_cost) as np_cost
            , sum(daily_costs.gp_psy_cost) as gp_psy_cost
            , sum(daily_costs.gp_other_cost) as gp_other_cost
            , sum(daily_costs.total_cost) as total_cost
        from months
        inner join daily_costs
            on daily_costs.date_day <@ months.month_range_est
        left join user_contract
            on daily_costs.date_day <@ user_contract.during_est
            and daily_costs.user_id = user_contract.user_id
        group by 1,2,3
    )

    , revenue as (
        select months.date_month
            , revenue_tmp.account_id
            , sum(amount) as revenue
        from months
        inner join revenue_tmp
            using (date_month)
        group by 1,2
    )

    , final as (
        select coalesce(costs.date_month, revenue.date_month)
                as date_month
            , coalesce(costs.account_id, revenue.account_id)
                as account_id
            , costs.account_name
            , revenue.revenue
            , costs.cc_cost
            , costs.nc_cost
            , costs.np_cost
            , costs.gp_psy_cost
            , costs.gp_other_cost
            , costs.total_cost
        from costs
        -- in case there are no costs for a given month
        full outer join revenue
            using (date_month, account_id)
    )

select
    -- produce a uid for date_month and account_id, and in the cases
    -- where there is no account, coalesce to n/a
    md5(
        date_month::text ||
        coalesce(account_id::text, 'n/a')
    ) as gm_id
    , *
from final
