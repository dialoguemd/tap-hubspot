with user_contract as (
        select * from {{ ref ( 'scribe_user_contract_detailed' ) }}
    )

    , contracts as (
        select * from {{ ref ( 'scribe_contracts_detailed' ) }}
    )

    , organizations as (
        select * from {{ ref ( 'organizations' ) }}
    )

    , daily_costs as (
        select * from {{ ref ( 'medops_est_costs_by_ep_daily' ) }}
    )

    , days_tmp as (
        select * from {{ ref ( 'dimension_days' ) }}
    )

    , days as (
        select * from days_tmp
        where date_day >= '2018-04-01'
    )
    
    , daily_revenue as (
        select days.date_day
            , contracts.contract_id
            , contracts.organization_name
            , organizations.account_name
            , coalesce(user_contract.residence_province, organizations.province)
                as residence_province
            , contracts.charge_strategy
            , case when contracts.charge_strategy = 'dynamic'
                    then contracts.charge_price
                    / days.days_in_month
                when contracts.charge_strategy = 'auto_dynamic'
                    then 15 / days.days_in_month
                when contracts.organization_id = '230'
                    then 1 / days.days_in_month
                when contracts.charge_strategy = 'fixed'
                    then 9 / days.days_in_month
                else 0
            end as charge_price
        from days
        inner join contracts
            on contracts.during @> days.date_day
        left join organizations
            using (organization_id)
        inner join user_contract using (contract_id)
        where user_contract.is_employee
    )

select coalesce(daily_revenue.date_day, daily_costs.date_day) as date_day
    , coalesce(daily_revenue.contract_id, 0) as contract_id
    , coalesce(daily_revenue.charge_price, 0) as charge_price
    , coalesce(daily_revenue.charge_strategy, 'free') as charge_strategy
    , coalesce(daily_revenue.organization_name, 'N/A') as organization_name
    , coalesce(daily_revenue.account_name, 'N/A') as account_name
    , coalesce(daily_revenue.residence_province, 'N/A') as residence_province
    , coalesce(sum(daily_costs.cc_cost), 0) as cc_cost
    , coalesce(sum(daily_costs.nc_cost), 0) as nc_cost
    , coalesce(sum(daily_costs.np_cost), 0) as np_cost
    , coalesce(sum(daily_costs.gp_psy_cost), 0) as gp_psy_cost
    , coalesce(sum(daily_costs.gp_other_cost), 0) as gp_other_cost
    , coalesce(sum(daily_costs.cc_cost), 0) +
        coalesce(sum(daily_costs.nc_cost), 0) +
        coalesce(sum(daily_costs.np_cost), 0) +
        coalesce(sum(daily_costs.gp_psy_cost), 0) +
        coalesce(sum(daily_costs.gp_other_cost), 0) as total_cost
from daily_revenue
left join user_contract using (contract_id)
-- TODO: some daily_costs are not joining so costs are understated
-- TODO: remove coalesce to N/A once all costs are joined to rev
full outer join daily_costs
    on user_contract.user_id = daily_costs.user_id
    and daily_revenue.date_day = daily_costs.date_day
group by 1,2,3,4,5,6,7
