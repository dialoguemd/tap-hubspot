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

	, costs_monthly as (
		select *
			, fl_gp_cost + fl_np_cost as cost_video
			, fl_nc_cost + fl_cc_cost
				-- License and SAAS costs are included for the year of 2019
				+ case
					when date_month >= '2019-01-01'
					then bonjour_sante_cost + licenses_cost + saas_cost
					else bonjour_sante_cost
				end
			as cost_chat
			, case
					when date_month >= '2019-01-01'
					then bonjour_sante_cost + licenses_cost + saas_cost
					else bonjour_sante_cost
				end
			as cost_other
			, fl_gp_cost + fl_np_cost + fl_nc_cost + fl_cc_cost
				-- License and SAAS costs are included for the year of 2019
				+ case
					when date_month >= '2019-01-01'
					then bonjour_sante_cost +licenses_cost + saas_cost
					else bonjour_sante_cost
				end
			as cost_total
		from finance
	)

	, monthly as (
		select *
			, active_users.daus_paid::float / active_users.daus as paid_users_rate
		from active_users
		inner join costs_monthly
			using (date_month)
		inner join active_contracts
			using (date_month)
	)

select monthly.*
	, coalesce(
			finance_rebate_monthly.rebate_percentage
			, 1
		) as rebate_percentage
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
		- monthly.cost_total *
		-- rebate on cost via seed file was deprecated in 2019-03-01
		coalesce(
			1 - finance_rebate_monthly.rebate_percentage
			, 1
		)
	) / monthly.telehealth_revenue as gm1
	, monthly.fl_nc_cost / monthly.daus as cost_to_serve_a_patient_nc
	, monthly.fl_cc_cost / monthly.daus as cost_to_serve_a_patient_cc
	, monthly.fl_gp_cost / monthly.daus as cost_to_serve_a_patient_gp
	, monthly.fl_np_cost / monthly.daus as cost_to_serve_a_patient_np
	, monthly.cost_other / monthly.daus as cost_to_serve_a_patient_other
	, monthly.cost_total / monthly.daus as cost_to_serve_a_patient
from monthly
left join finance_rebate_monthly
	using (date_month)
