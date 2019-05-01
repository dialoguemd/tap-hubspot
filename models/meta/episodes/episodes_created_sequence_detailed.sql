with
	sequence as (
		select * from {{ ref('countdown_episode_started_sequence') }}
	)

	, videos_detailed as (
		select * from {{ ref('videos_detailed') }}
	)

	, phone_calls_detailed as (
		select * from {{ ref('telephone_calls_detailed') }}
	)

	, videos as (
		select episode_id
			, min(started_at_est) as first_video_cc_nc_started_at
			, min(ended_at_est) as first_video_cc_nc_ended_at
		from videos_detailed
		-- Only include triage videos, not virtual consultation videos
		where main_specialization in ('Nurse Clinican', 'Care Coordinator')
		group by 1
	)

	, phone_calls as (
		select episode_id
			, min(started_at_est) as first_phone_call_cc_nc_started_at
			, min(ended_at_est) as first_phone_call_cc_nc_ended_at
		from phone_calls_detailed
		where main_specialization in ('Nurse Clinican', 'Care Coordinator')
		group by 1
	)

select sequence.episode_id
	, sequence.channel_selected
	, sequence.appointment_preference
	, sequence.dxa_started_at
	, sequence.dxa_started_at is not null as is_dxa_started
	, sequence.dxa_completed_at
	, sequence.dxa_completed_at is not null as is_dxa_completed
	, sequence.dxa_resume_count
	, sequence.channel_select_started_at
	, sequence.channel_select_completed_at
	, videos.first_video_cc_nc_started_at
	, videos.first_video_cc_nc_ended_at
	, phone_calls.first_phone_call_cc_nc_started_at
	, phone_calls.first_phone_call_cc_nc_ended_at
	, extract(epoch from
		sequence.dxa_completed_at
		- sequence.dxa_started_at
		) / 60.0
	as dxa_completion_time
from sequence
left join videos
	using (episode_id)
left join phone_calls
	using (episode_id)
