select date_month
	, fl_gp_cost::float as fl_gp_cost
	, fl_np_cost::float as fl_np_cost
	, fl_nc_cost::float as fl_nc_cost
	, fl_cc_cost::float as fl_cc_cost
	, other_cost::float as other_cost
from {{ ref('data_medops_fl_costs_by_main_spec') }}

