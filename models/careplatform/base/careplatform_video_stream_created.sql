select timestamp as created_at
    , episode_id
    , user_id as practitioner_id
from careplatform.video_patient_stream_created
