with
	reminder_updated as (
		select * from {{ ref('careplatform_reminders_status_updated') }}
	)

	, reminder_created as (
        select * from {{ ref('careplatform_reminder_created') }}
    )

	, reminders_completed as (
		select reminder_id
			, min(timestamp_est) as completed_at_est
			, min(date_day_est) as completed_at_day_est
		from reminder_updated
		where reminder_status = 'completed'
		group by 1
	)

	, reminders as (
		select reminder_created.reminder_id
			, reminder_created.episode_id
			, reminder_created.due_at_est
			, reminders_completed.completed_at_est
			-- Generate a series of days where the reminder was open
			, generate_series(
				reminder_created.date_day_est,
				reminders_completed.completed_at_day_est,
				'1 day'
			) as date_day_est
		from reminder_created
		left join reminders_completed
			using (reminder_id)
		where -- exclude same day reminders
			reminder_created.due_at_day_est
			<> reminders_completed.completed_at_day_est
			and reminder_created.date_day_est
			<= reminders_completed.completed_at_day_est
	)

select episode_id
	, date_day_est
	, count(*) > 0 as has_open_reminder
from reminders
group by 1,2
