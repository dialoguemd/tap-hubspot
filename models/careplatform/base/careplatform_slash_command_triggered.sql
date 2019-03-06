select episode_id
    , command_name
    , user_id
    , command_id
    , timestamp as triggered_at
    , timestamp
from careplatform.executed_command
