{{
  config(
    materialized='incremental',
    post_hook=[
       "DROP INDEX IF EXISTS {{ this.schema }}.index_gm_by_contract_date_day",
       "CREATE INDEX IF NOT EXISTS index_gm_by_contract_date_day ON {{ this }}(date_day)"
    ]
  )
}}

with user_contract as (
        select * from {{ ref ( 'user_contract' ) }}
    )

    , daily_costs as (
        select * from {{ ref ( 'costs_by_episode_daily' ) }}
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
            , user_contract.contract_id
            , user_contract.organization_name
            , user_contract.account_name
            , user_contract.residence_province
            , user_contract.charge_strategy
            , case when user_contract.charge_strategy = 'dynamic'
                    then user_contract.charge_price
                    / days.days_in_month
                when user_contract.charge_strategy = 'auto_dynamic'
                    then 15 / days.days_in_month
                when user_contract.organization_id = '230'
                    then 1 / days.days_in_month
                when user_contract.charge_strategy = 'fixed'
                    then 9 / days.days_in_month
                else 0
            end as charge_price
        from days
        inner join user_contract
            on days.date_day <@ user_contract.during
        where user_contract.is_employee
            and days.date_day < current_date

            -- Filtering for incremental model
            {% if is_incremental() %}
            and days.date_day > (select max(date_day) from {{ this }})
            {% endif %}
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
