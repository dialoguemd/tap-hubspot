with
	apt_booking as (
		select * from {{ ref('careplatform_appointment_booking_started') }}
	)

	, video_started as (
		select * from {{ ref('videos_started') }}
	)

	, joined as (
		select apt_booking.episode_id
			, apt_booking.timestamp as triggered_at
			, video_started.timestamp as video_started
			, extract(epoch from video_started.timestamp
				- apt_booking.timestamp)::float / 3600 as time_to_next_apt_hr
			, row_number() over (partition by
				concat(apt_booking.episode_id, apt_booking.timestamp)
				order by video_started.timestamp) as rank
		from apt_booking
		inner join video_started
			on apt_booking.episode_id = video_started.episode_id
			and apt_booking.timestamp < video_started.timestamp
		where main_specialization in ('Family Physician', 'Nurse Practitioner')
	)

select episode_id
	, date_trunc('month', triggered_at) as triggered_at_month
	, date_trunc('week', triggered_at) as triggered_at_week
	, date_trunc('day', triggered_at) as triggered_at_day
	, triggered_at
	, date_trunc('month', video_started) as video_started_month
	, date_trunc('week', video_started) as video_started_week
	, date_trunc('day', video_started) as video_started_day
	, video_started
	, time_to_next_apt_hr
from joined
where rank = 1
