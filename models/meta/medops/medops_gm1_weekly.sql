with
	costs as (
		select * from {{ ref('wiw_costs_weekly')}}
	)

	, contracts as (
		select * from {{ ref('scribe_contracts_weekly') }}
	)

	, active_users as (
		select * from {{ ref('active_users_weekly') }}
	)

select contracts.date_week
	-- FIXME: replace ARPU with a dynamic version
	, ((contracts.contract_count * 7.0 / 4.33)
		- costs.virtual_costs_fl)
		/ (contracts.contract_count * 7.0 / 4.33) as gm1
	, contracts.contract_paid_count

	, case
		when active_users.daus = 0
		then 0
		else costs.virtual_costs_gp_fl / active_users.daus
	end as cost_per_patient_gp
	, case
		when active_users.daus = 0
		then 0
		else costs.virtual_costs_np_fl / active_users.daus
	end as cost_per_patient_np
	, case
		when active_users.daus = 0
		then 0
		else costs.virtual_costs_nc_fl / active_users.daus
	end as cost_per_patient_nc
	, case
		when active_users.daus = 0
		then 0
		else costs.virtual_costs_cc_fl / active_users.daus
	end as cost_per_patient_cc
	, case
		when active_users.daus_chat = 0
		then 0
		else costs.virtual_chat_costs_fl / active_users.daus_chat
	end as cost_per_chat
	, case
		when active_users.daus_video_gp_np = 0
		then 0
		else costs.virtual_video_costs_fl / active_users.daus_video_gp_np
	end as cost_per_video
from contracts
inner join active_users
	using(date_week)
inner join costs
	on contracts.date_week = costs.start_week
