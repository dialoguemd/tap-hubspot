select episode_id
    , command_name
    , user_id
    , command_id
    , timestamp as triggered_at
    , timestamp
    , timezone('America/Montreal', timestamp) as timestamp_est
from careplatform.executed_command
