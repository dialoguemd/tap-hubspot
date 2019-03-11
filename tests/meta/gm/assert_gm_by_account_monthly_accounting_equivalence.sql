-- Because of the possibility of multiple active contracts this test checks to make sure
-- costs aren't drastically overstated in this GM monthly model

with costs as (
      select * from {{ ref( 'finance_revenue_and_costs_monthly' ) }}
   )

   , gm_monthly as (
      select * from {{ ref( 'gm_by_account_monthly' ) }}
   )

   , monthly_costs as (
      select date_month
          , sum(cc_cost) as cc_cost
          , sum(nc_cost) as nc_cost
          , sum(np_cost) as np_cost
          , sum(gp_psy_cost) as gp_psy_cost
          , sum(gp_other_cost) as gp_other_cost
      from gm_monthly
      group by 1
   )

    , compared as (
      select date_month
          , round(((gp_psy_cost + gp_other_cost) / fl_gp_cost)::numeric, 3) as gp
          , round((cc_cost / fl_cc_cost)::numeric, 3) as cc
          , round((nc_cost / fl_nc_cost)::numeric, 3) as nc
          , round((np_cost / fl_np_cost)::numeric, 3) as np
      from costs
      left join monthly_costs using (date_month)
      -- Filter for after April only because of video refactor and tracking changes
      where date_month > '2018-04-01'
      {% if target.name == 'dev' %}
        and date_month > current_date - interval '2 months'
      {% endif %}
    )

select *
from compared
-- Calibrated on March 11, 2019
where (gp > 1.005 or cc > 1.005 or nc > 1.005 or np > 1.010)
