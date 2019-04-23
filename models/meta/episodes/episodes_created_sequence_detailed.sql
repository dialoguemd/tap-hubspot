with
	sequence as (
		select * from {{ ref('countdown_episode_started_sequence') }}
	)

	, videos_detailed as (
		select * from {{ ref('videos_detailed') }}
	)

	, videos as (
		select episode_id
			, min(started_at_est) as video_started_at
			, min(ended_at_est) as video_ended_at
		from videos_detailed
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
	, videos.video_started_at
	, videos.video_ended_at
	, extract(epoch from
		sequence.dxa_completed_at
		- sequence.dxa_started_at
		) / 60.0
	as dxa_completion_time
from sequence
left join videos
	using (episode_id)
