select timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
	, date_trunc('day', timestamp) as date_day
	, date_trunc(
		'day', timezone('America/Montreal', timestamp)
	) as date_day_est
    , reminder_id
    , reminder_episode_id as episode_id
    , reminder_due_at as due_at
    , timezone('America/Montreal', reminder_due_at) as due_at_est
	, date_trunc('day', reminder_due_at) as due_at_day
	, date_trunc(
		'day', timezone('America/Montreal', timestamp)
	) as due_at_day_est
	, user_id
from careplatform.reminders_create_new_success
