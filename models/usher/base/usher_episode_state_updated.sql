select episode_id
  , timestamp as updated_at
  , episode_state
  , user_id
from usher.set_episode_state
