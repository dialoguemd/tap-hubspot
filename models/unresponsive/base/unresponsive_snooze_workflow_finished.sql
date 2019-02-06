select timestamp as workflow_finished_at
	, timezone('America/Montreal', timestamp) as timestamp_est
	, date_trunc('day', timestamp) as date_day
	, date_trunc('day', timezone('America/Montreal', timestamp)) as date_day_est
	, user_id
	, episode_id
from unresponsive.giveup
