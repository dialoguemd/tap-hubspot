select timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
    , episode_id
    , user_id as practitioner_id
from careplatform.video_destroy_session
