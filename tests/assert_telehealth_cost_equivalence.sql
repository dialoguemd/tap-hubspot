with seed as (
      select * from {{ ref( 'medops_fl_costs_by_main_spec' ) }}
   )
   
   , costs_by_ep as (
      select * from {{ ref( 'medops_est_costs_by_ep_daily' ) }}  
   )
   
   , monthly_costs as (
      select date_trunc('month', date_day) as month
          , sum(cc_cost) as cc_cost
          , sum(nc_cost) as nc_cost
          , sum(np_cost) as np_cost
          , sum(gp_psy_cost) as gp_psy_cost
          , sum(gp_other_cost) as gp_other_cost
      from costs_by_ep
      group by 1
   )
   
    , compared as (
      select round(((gp_psy_cost + gp_other_cost) / fl_gp_cost)::numeric, 4) as gp
          , round(cc_cost / fl_cc_cost, 4) as cc
          , round(nc_cost / fl_nc_cost, 4) as nc
          , round(np_cost / fl_np_cost, 4) as np
      from seed
      left join monthly_costs using (month)
    )

select *
from compared
where (gp <> 1 or cc <>1 or nc <> 1 or np <> 1)
