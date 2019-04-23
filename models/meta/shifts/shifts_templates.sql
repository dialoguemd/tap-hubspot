with
	shifts as (
		select * from {{ ref('wiw_shifts') }}
	)

	, templates as (
		select * from {{ ref('careplatform_slash_command_triggered') }}
	)

select shifts.shift_id
	, count(distinct templates.episode_id)
		filter (where command_id = 'Appointment Virtual')
		as appointments_booked_count
from shifts
left join templates
	on shifts.shift_schedule_est @> templates.timestamp_est
	and shifts.user_id = templates.user_id
group by 1
