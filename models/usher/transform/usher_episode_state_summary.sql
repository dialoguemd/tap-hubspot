with
	episode_state_updated as (
		select * from {{ ref('usher_episode_state_updated') }}
	)

select episode_id
	, min(timestamp) as first_state_updated
	, min(timestamp_est) as first_state_updated_est
	, min(date_day) as first_state_updated_date_day
	, min(date_day_est) as first_state_updated_date_day_est
from episode_state_updated
group by 1
