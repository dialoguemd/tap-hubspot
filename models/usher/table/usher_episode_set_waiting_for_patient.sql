with
	episode_state_updated as (
		select * from {{ ref('usher_episode_state_updated') }}
	)

select episode_id
  , timestamp
  , timestamp_est
  , date_day
  , date_day_est
  , user_id
from episode_state_updated
where episode_state = 'waiting_for_patient'
