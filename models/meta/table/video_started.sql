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
			, patient_id
			, episode_id
			, user_id as careplatform_user_id
		from video_stream_created
		where timestamp > '2018-04-10'

		union all

		select cp_video_session_remote_connected.timestamp
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
			, user_id as patient_id
			, null as episode_id
			, null as careplatform_user_id
		from video_session_connect_success
		where timestamp < '2017-11-01'
	)

select videos.*
	, coalesce(practitioners.main_specialization, 'N/A') as main_specialization
from videos
left join practitioners
	on videos.careplatform_user_id = practitioners.user_id
