select episode_id
  , timestamp as updated_at
  , timezone('America/Montreal', timestamp) as timestamp_est
  , date_trunc('day', timestamp) as date_day
  , date_trunc('day', timezone('America/Montreal', timestamp)) as date_day_est
  , episode_state
  , user_id
from usher.set_episode_state
