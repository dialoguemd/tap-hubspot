with
    user_contract as (
        select * from {{ ref ( 'user_contract' ) }}
    )

    , daily_costs as (
        select * from {{ ref ( 'costs_by_episode_daily' ) }}
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
            , user_contract.organization_id
            , user_contract.residence_province
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
            , user_contract.organization_id
            , user_contract.organization_name
            , user_contract.account_id
            , user_contract.account_name
            , user_contract.charge_strategy
            , user_contract.residence_province
            , count(distinct user_contract.user_id) as active_contract_count
            , sum(
                case when user_contract.charge_strategy = 'dynamic'
                    then user_contract.charge_price
                when user_contract.charge_strategy = 'auto_dynamic'
                    then 15
                when user_contract.organization_id = '230'
                    then 1
                when user_contract.charge_strategy = 'fixed'
                    then 9
                else 0
            end) as revenue
        from months
        inner join user_contract
            on months.month_range_est && user_contract.during_est
        {{ dbt_utils.group_by(n=7) }}
    )

    , final as (
        select coalesce(costs.date_month, revenue.date_month)
                as date_month
            , coalesce(costs.organization_id, revenue.organization_id)
                as organization_id
            , coalesce(costs.residence_province, revenue.residence_province)
                as residence_province
            , revenue.organization_name
            , revenue.account_id
            , revenue.account_name
            , revenue.charge_strategy
            , revenue.revenue
            , revenue.active_contract_count
            , costs.cc_cost
            , costs.nc_cost
            , costs.np_cost
            , costs.gp_psy_cost
            , costs.gp_other_cost
            , costs.total_cost
        from costs
        -- in case there are no costs for a given month
        full outer join revenue
            using (date_month, organization_id, residence_province)
    )

select
    -- produce a uid for date_month, org_id, and province, and in the cases
    -- where there is no org, coalesce to n/a
    md5(
        date_month::text ||
        coalesce(organization_id::text, 'n/a') ||
        coalesce(residence_province, 'n/a')
    ) as gm_id
    , *
from final
