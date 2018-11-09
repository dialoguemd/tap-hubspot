with
	videos as (
		select * from {{ ref('video_started') }}
	)

	, episodes_subject as (
		select * from {{ ref('episodes_subject') }}
	)

select date_trunc('day', timezone('America/Montreal', videos.timestamp)) as date_day
	, coalesce(episodes_subject.episode_subject, videos.patient_id) as patient_id
	, video_started.episode_id
	, bool_or(main_specialization = 'Family Physician') as includes_video_gp
	, bool_or(main_specialization = 'Nurse Practitioner') as includes_video_np
	, bool_or(main_specialization = 'Nurse Clinician') as includes_video_nc
	, bool_or(main_specialization = 'Care Coordinator') as includes_video_cc
	, bool_or(main_specialization = 'N/A') as includes_video_unidentified
	, min(timestamp) as first_timestamp
from videos
left join episodes_subject
	using (episode_id)
group by 1,2,3
