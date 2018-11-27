select timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
    , user_id as practitioner_id
    , episode_id
    , patient_id
from careplatform.video_patient_stream_created
