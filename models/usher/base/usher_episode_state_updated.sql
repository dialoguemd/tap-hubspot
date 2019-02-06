select episode_id
  , timestamp
  , timezone('America/Montreal', timestamp) as timestamp_est
  , date_trunc('day', timestamp) as date_day
  , date_trunc('day', timezone('America/Montreal', timestamp)) as date_day_est
  , episode_state
  , user_id
from usher.set_episode_state
union all
select episode_id
  , timestamp
  , timezone('America/Montreal', timestamp) as timestamp_est
  , date_trunc('day', timestamp) as date_day
  , date_trunc('day', timezone('America/Montreal', timestamp)) as date_day_est
  , 'waiting_for_patient' as episode_state
  , user_id
from unresponsive.nudge
-- date when fix for episode set to waiting_for_patient was released
where timestamp < '2019-01-29 19:38:04.704+00'
	and iteration = 1
