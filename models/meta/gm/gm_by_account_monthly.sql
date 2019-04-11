with
    user_contract as (
        select * from {{ ref('user_contract') }}
    )

    , daily_costs as (
        select * from {{ ref('costs_by_episode_daily') }}
    )

    , revenue as (
        select * from {{ ref('finance_revenue_adjusted_by_account_monthly_v2') }}
    )

    , months as (
        select * from {{ ref('dimension_months') }}
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
            on daily_costs.date_day <@ months.month_range
        left join user_contract
            on daily_costs.date_day <@ user_contract.during_est
            and daily_costs.user_id = user_contract.user_id
        group by 1,2,3
    )

    , final as (
        select coalesce(costs.date_month, revenue.date_month)
                as date_month
            , coalesce(costs.account_id, revenue.account_id, 'n/a')
                as account_id
            , costs.account_name
            , revenue.amount as revenue
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
        coalesce(account_id)
    ) as gm_id
    , *
from final
