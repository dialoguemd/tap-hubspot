select episode_id
  , timestamp as updated_at
  , episode_state
from usher.set_episode_state
