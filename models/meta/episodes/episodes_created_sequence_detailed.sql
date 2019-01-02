with
	sequence as (
		select * from {{ ref('countdown_episode_started_sequence') }}
	)

	, videos_detailed as (
		select * from {{ ref('videos') }}
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
	, sequence.dxa_started_at
	, sequence.dxa_completed_at
	, sequence.channel_select_started_at
	, sequence.channel_select_completed_at
	, videos.video_started_at
	, videos.video_ended_at
from sequence
left join videos
	using (episode_id)
