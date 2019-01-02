with
	command_triggered as (
		select * from {{ ref('careplatform_slash_command_triggered') }}
	)

	, video_started as (
		select * from {{ ref('videos_started') }}
	)

	, joined as (
		select command_triggered.episode_id
			, command_triggered.triggered_at
			, video_started.timestamp as video_started
			, extract(epoch from video_started.timestamp
				- command_triggered.triggered_at)::float / 3600 as time_to_next_apt_hr
			, row_number() over (partition by
				concat(command_triggered.episode_id, command_triggered.triggered_at)
				order by video_started asc) as rank
		from command_triggered
		inner join video_started
			on command_triggered.episode_id = video_started.episode_id
			and command_triggered.triggered_at < video_started.timestamp
		where command_name = 'templates'
			and command_id = 'Appointment Virtual'
			and main_specialization in ('Family Physician', 'Nurse Practitioner')
	)

select	episode_id
	, triggered_at
	, video_started
	, time_to_next_apt_hr
from joined
where rank = 1
