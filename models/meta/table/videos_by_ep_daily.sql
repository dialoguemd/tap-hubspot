with
	videos as (
		select * from {{ ref('videos_detailed') }}
	)

select videos.date_day_est
	, videos.episode_id
	, videos.patient_id
	, bool_or(videos.main_specialization = 'Family Physician')
		as includes_video_gp
	, bool_or(videos.main_specialization = 'Nurse Practitioner')
		as includes_video_np
	, bool_or(videos.main_specialization = 'Nurse Clinician')
		as includes_video_nc
	, bool_or(videos.main_specialization = 'Care Coordinator')
		as includes_video_cc
	, bool_or(videos.main_specialization = 'N/A')
		as includes_video_unidentified
	, min(videos.started_at_est) as first_timestamp_est
	, sum(videos.video_length) as video_length
from videos
group by 1,2,3
