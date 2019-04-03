-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

--

with
    est_costs_daily as (
        select * from {{ ref('costs_by_episode_daily') }}
    )

    , episodes as (
        select * from {{ ref('episodes_with_contracts') }}
    )

select episodes.organization_name
    , episodes.account_name
    , date_trunc('month', est_costs_daily.date_day) as date_month
    , coalesce(sum(est_costs_daily.cc_cost),0) as cc_cost
    , coalesce(sum(est_costs_daily.nc_cost),0) as nc_cost
    , coalesce(sum(est_costs_daily.np_cost),0) as np_cost
    , coalesce(sum(est_costs_daily.gp_psy_cost),0) as gp_psy_cost
    , coalesce(sum(est_costs_daily.gp_other_cost),0) as gp_other_cost
from est_costs_daily
inner join episodes using(episode_id)
group by 1,2,3
