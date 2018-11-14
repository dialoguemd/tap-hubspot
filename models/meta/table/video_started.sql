with
	video_session_connect_success as (
		select * from {{ ref('patientapp_video_session_connect_success') }}
	)

	, cp_video_session_remote_connected as (
		select * from {{ ref('careplatform_cp_video_session_remote_connected') }}
	)

	, channels as (
		select * from {{ ref('messaging_channels') }}
	)

	, video_stream_created as (
		select * from {{ ref('careplatform_video_stream_created') }}
	)

	, practitioners as (
		select * from {{ ref('coredata_practitioners')}}
	)

	, videos as (
		select timestamp
			, timezone('America/Montreal',timestamp) as timestamp_est
			, patient_id
			, episode_id
			, user_id as careplatform_user_id
		from video_stream_created
		where timestamp > '2018-04-10'

		union all

		select timestamp
			, timezone('America/Montreal',
				cp_video_session_remote_connected.timestamp) as timestamp_est
			, channels.user_id as patient_id
			, channels.episode_id
			, cp_video_session_remote_connected.user_id as careplatform_user_id
		from cp_video_session_remote_connected
		inner join channels
			using (episode_id)
		where timestamp <= '2018-04-10'
			and timestamp > '2017-11-01'

		union all

		select timestamp
			, timezone('America/Montreal',timestamp) as timestamp_est
			, user_id as patient_id
			, null as episode_id
			, null as careplatform_user_id
		from video_session_connect_success
		where timestamp < '2017-11-01'
	)

select videos.careplatform_user_id
	, date_trunc('day',
		timezone('America/Montreal',timestamp))::timestamp as date_day_est
	, date_trunc('day', timestamp)::timestamptz as date_day
	, videos.timestamp_est
	, videos.timestamp
	, 'video'::text as activity
	, videos.patient_id
	, videos.episode_id
	, coalesce(practitioners.main_specialization, 'N/A') as main_specialization
from videos
left join practitioners
	on videos.careplatform_user_id = practitioners.user_id
