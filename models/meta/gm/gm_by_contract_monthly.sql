with
    gm_by_contract_daily as (
        select * from {{ ref('gm_by_contract_daily') }}
    )

select date_trunc('month', date_day) as date_month
    , contract_id
    , charge_strategy
    , organization_name
    , account_name
    , residence_province
    , sum(charge_price) as charge_price
    , sum(cc_cost) as cc_cost
    , sum(nc_cost) as nc_cost
    , sum(np_cost) as np_cost
    , sum(gp_psy_cost) as gp_psy_cost
    , sum(gp_other_cost) as gp_other_cost
    , sum(total_cost) as total_cost
from gm_by_contract_daily
{{ dbt_utils.group_by(n=6) }}
