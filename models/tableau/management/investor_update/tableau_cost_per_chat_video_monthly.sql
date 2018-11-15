with
	active_users as (
		select * from {{ ref('active_users_monthly')}}
	)

	, revenue as (
		select * from {{ ref('finance_revenue_monthly') }}
	)

	, costs as (
		select * from {{ ref('medops_fl_costs_by_main_spec') }}
	)

	, active_contracts as (
		select * from {{ ref('scribe_active_contracts_monthly') }}
	)

	, monthly as (
		select *
			, costs.fl_gp_cost + costs.fl_np_cost as cost_video
			, costs.fl_nc_cost + costs.fl_cc_cost + costs.other_cost as cost_chat
			, costs.fl_gp_cost + costs.fl_np_cost + costs.fl_nc_cost
				+ costs.fl_cc_cost + costs.other_cost as cost_total
			, active_users.daus_paid::float / active_users.daus as paid_users_rate
		from active_users
		inner join costs
			using (date_month)
		inner join revenue
			using (date_month)
		inner join active_contracts
			using (date_month)
	)

select *
	, case
		when daus_chat <> 0
		then cost_chat / daus_chat
		else 0
	end as cost_per_chat
	, case
		when daus_video_gp_np <> 0
		then cost_video / daus_video_gp_np
		else 0
	end as cost_per_video
	, cost_total / active_contracts
		as cost_to_serve_a_member
	, telehealth_revenue / active_contracts_paid as arpu
	, (telehealth_revenue - cost_total * paid_users_rate)
		/ telehealth_revenue as gm1
	, fl_nc_cost / daus as cost_to_serve_a_patient_nc
	, fl_cc_cost / daus as cost_to_serve_a_patient_cc
	, fl_gp_cost / daus as cost_to_serve_a_patient_gp
	, fl_np_cost / daus as cost_to_serve_a_patient_np
	, other_cost / daus as cost_to_serve_a_patient_other
	, cost_total / daus as cost_to_serve_a_patient
from monthly
