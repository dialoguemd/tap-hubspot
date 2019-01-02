select timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
	, date_trunc('day', timestamp) as date_day
	, date_trunc(
		'day', timezone('America/Montreal', timestamp)
	) as date_day_est
	, reminder_id
	, user_id
	, reminder_episode_id as episode_id
	, reminder_due_at as due_at
	, reminder_status
from careplatform.reminders_status_change_success
