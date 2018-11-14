-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with est_costs_daily as (
        select * from {{ ref( 'medops_est_costs_by_ep_daily' ) }}
    )

    , organizations as (
        select * from {{ ref( 'organizations' ) }}
    )

    , episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

select episodes.organization_name
    , organizations.account_name
    , date_trunc('month', est_costs_daily.date_day) as month
    , coalesce(sum(est_costs_daily.cc_cost),0) as cc_cost
    , coalesce(sum(est_costs_daily.nc_cost),0) as nc_cost
    , coalesce(sum(est_costs_daily.np_cost),0) as np_cost
    , coalesce(sum(est_costs_daily.gp_psy_cost),0) as gp_psy_cost
    , coalesce(sum(est_costs_daily.gp_other_cost),0) as gp_other_cost
from est_costs_daily
-- Full outer for edge cases of no other ep costs on the given day
inner join episodes using(episode_id)
inner join organizations using (organization_id)
group by 1,2,3
