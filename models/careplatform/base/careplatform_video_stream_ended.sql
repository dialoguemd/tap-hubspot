select timestamp as ended_at
    , episode_id
    , user_id as practitioner_id
from careplatform.video_platform_ended_call
