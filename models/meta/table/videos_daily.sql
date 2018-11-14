with
	videos as (
		select * from {{ ref('video_started') }}
	)

	, episodes_subject as (
		select * from {{ ref('episodes_subject') }}
	)

select date_trunc('day', videos.timestamp_est) as date_day
	, coalesce(episodes_subject.episode_subject, videos.patient_id) as patient_id
	, bool_or(main_specialization = 'Family Physician') as includes_video_gp
	, bool_or(main_specialization = 'Nurse Practitioner') as includes_video_np
	, bool_or(main_specialization = 'Nurse Clinician') as includes_video_nc
	, bool_or(main_specialization = 'Care Coordinator') as includes_video_cc
	, bool_or(main_specialization = 'N/A') as includes_video_unidentified
	, min(timestamp_est) as first_timestamp
from videos
left join episodes_subject
	using (episode_id)
group by 1,2
