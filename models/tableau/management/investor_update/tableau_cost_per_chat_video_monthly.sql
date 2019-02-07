with
	active_users as (
		select * from {{ ref('active_users_monthly')}}
	)

	, finance as (
		select * from {{ ref('finance_revenue_and_costs_monthly') }}
	)

	, active_contracts as (
		select * from {{ ref('scribe_active_contracts_monthly') }}
	)

	, finance_rebate_monthly as (
		select * from {{ ref('finance_rebate_monthly') }}
	)

	, monthly as (
		select *
			, finance.fl_gp_cost + finance.fl_np_cost as cost_video
			, finance.fl_nc_cost + finance.fl_cc_cost + finance.other_cost as cost_chat
			, finance.fl_gp_cost + finance.fl_np_cost + finance.fl_nc_cost
				+ finance.fl_cc_cost + finance.other_cost as cost_total
			, active_users.daus_paid::float / active_users.daus as paid_users_rate
		from active_users
		inner join finance
			using (date_month)
		inner join active_contracts
			using (date_month)
	)

select monthly.*
	, case
		when monthly.daus_chat <> 0
		then monthly.cost_chat / monthly.daus_chat
		else 0
	end as cost_per_chat
	, case
		when monthly.daus_video_gp_np <> 0
		then monthly.cost_video / monthly.daus_video_gp_np
		else 0
	end as cost_per_video
	, monthly.cost_total / monthly.active_contracts
		as cost_to_serve_a_member
	, monthly.telehealth_revenue / monthly.active_contracts_paid as arpu
	, (
		monthly.telehealth_revenue
		- monthly.cost_total * coalesce(
			monthly.paid_users_rate
			, 1-finance_rebate_monthly.rebate_percentage
		)
	) / monthly.telehealth_revenue as gm1
	, monthly.fl_nc_cost / monthly.daus as cost_to_serve_a_patient_nc
	, monthly.fl_cc_cost / monthly.daus as cost_to_serve_a_patient_cc
	, monthly.fl_gp_cost / monthly.daus as cost_to_serve_a_patient_gp
	, monthly.fl_np_cost / monthly.daus as cost_to_serve_a_patient_np
	, monthly.other_cost / monthly.daus as cost_to_serve_a_patient_other
	, monthly.cost_total / monthly.daus as cost_to_serve_a_patient
from monthly
left join finance_rebate_monthly
	using (date_month)
