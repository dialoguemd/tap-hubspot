{% set overhead = 1.25 %}
with
	costs_daily as (
		select * from {{ ref('wiw_costs_by_position_daily') }}
	)

select start_date
	, coalesce(
		sum(virtual_costs) filter(where position_name = 'GP')
		+ {{ overhead }}
			* sum(virtual_costs) filter(where position_name <> 'GP')
		, 0
	) as virtual_costs_fl

	, coalesce(
		sum(virtual_costs)
			filter(where position_name = 'GP')
		+ {{ overhead }} * sum(virtual_costs)
			filter(where position_name = 'Nurse Practitioner')
		, 0
	) as virtual_video_costs_fl
	, {{ overhead }} * coalesce(
		sum(virtual_costs)
			filter(where position_name not in ('GP', 'Nurse Practitioner'))
		, 0
	) as virtual_chat_costs_fl

	, coalesce(sum(virtual_costs)
			filter(where position_name = 'GP')
		, 0
	) as virtual_costs_gp_fl
	, {{ overhead }} * coalesce(sum(virtual_costs)
			filter(where position_name = 'Nurse Practitioner' )
		, 0
	) as virtual_costs_np_fl
	, {{ overhead }} * coalesce(sum(virtual_costs)
			filter(where position_name in ('Nurse', 'Triage',
				'Night Shift Nurse', 'Evening Shift Nurse'))
		, 0
	) as virtual_costs_nc_fl
	, {{ overhead }} * coalesce(sum(virtual_costs)
			filter(where position_name not in (
				'Nurse', 'Triage', 'Night Shift Nurse',
				'Evening Shift Nurse', 'Nurse Practitioner', 'GP'))
		, 0
	) as virtual_costs_cc_fl
from costs_daily
group by 1
