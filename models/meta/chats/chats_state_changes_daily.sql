with
	state_changed as (
		select * from {{ ref('usher_episode_state_updated') }}
	)

select episode_id
	, date_day_est
	, min(timestamp_est) filter (where episode_state = 'active')
		as first_set_active
	, min(timestamp_est) filter (where episode_state in ('pending', 'resolved'))
		as first_set_resolved_pending_at
	, min(timestamp_est) filter (where episode_state in ('pending', 'resolved'))
		is not null as set_resolved_pending
from state_changed
group by 1,2
