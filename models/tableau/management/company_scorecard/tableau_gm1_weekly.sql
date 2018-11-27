with
	gm as (
		select * from {{ ref('medops_gm1_weekly')}}
	)

	, dates as (
		select * from {{ ref('dimension_scorecard_weeks')}}
	)

select dates.date_week
	, gm.gm1
	, gm.contract_paid_count
	, gm.cost_per_patient_gp
	, gm.cost_per_patient_np
	, gm.cost_per_patient_nc
	, gm.cost_per_patient_cc
	, gm.cost_per_chat
	, gm.cost_per_video
from dates
left join gm
	on dates.date_week = gm.date_week
		and gm.date_week < date_trunc('week', current_date)
