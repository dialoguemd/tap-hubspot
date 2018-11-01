select cp_video_session_remote_connected.user_id
    , date_trunc('day', timezone('UTC', timestamp)) as date
    , timezone('UTC', timestamp) as timestamp
    , 'video'::text as activity
    , null::text as patient_id
    , episode_id
from careplatform.cp_video_session_remote_connected
where timestamp < '2018-04-10'
union all
select video_patient_stream_created.user_id
    , date_trunc('day',
            timezone(
                'UTC',
                video_patient_stream_created.timestamp
            )
        ) as date
    , timezone('UTC', video_patient_stream_created.timestamp) as timestamp
    , 'video'::text as activity
    , patient_id
    , episode_id
from careplatform.video_patient_stream_created
where timestamp > '2018-04-10'
