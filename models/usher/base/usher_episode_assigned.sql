select episode_id
  , timestamp as assigned_at
  , timestamp
  , assigned_user_id
  , user_id
from usher.episode_assigned
