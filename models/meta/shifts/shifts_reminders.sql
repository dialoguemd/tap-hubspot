with
	shifts as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

	, reminders as (
		select * from {{ ref('careplatform_reminders_status_updated') }}
	)

select shifts.shift_id
	, count(distinct reminders.reminder_id) as reminders_completed_count
from shifts
left join reminders
	on shifts.shift_schedule_est @> reminders.timestamp_est
	and shifts.user_id = reminders.user_id
where reminder_status = 'completed'
group by 1
