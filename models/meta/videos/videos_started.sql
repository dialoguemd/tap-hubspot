with
	video_session_connect_success as (
		select * from {{ ref('patientapp_video_session_connect_success') }}
	)

	, cp_video_session_remote_connected as (
		select * from {{ ref('careplatform_cp_video_session_remote_connected') }}
	)

	, video_stream_created as (
		select * from {{ ref('careplatform_video_stream_created') }}
	)

	, episodes_subject as (
		select * from {{ ref('episodes_subject') }}
	)

	, episodes_issue_types as (
		select * from {{ ref('episodes_issue_types') }}
	)

	, practitioners as (
		select * from {{ ref('practitioners')}}
	)

	, test_users as (
		select * from {{ ref('scribe_test_users')}}
	)

	, videos as (
		select timestamp
			, timezone('America/Montreal',timestamp) as timestamp_est
			, patient_id
			, episode_id
			, practitioner_id as careplatform_user_id
		from video_stream_created
		where timestamp > '2018-04-10'

		union all

		select cp_video_session_remote_connected.timestamp
			, timezone('America/Montreal',
				cp_video_session_remote_connected.timestamp) as timestamp_est
			, episodes_subject.episode_subject as patient_id
			, episodes_subject.episode_id
			, cp_video_session_remote_connected.user_id as careplatform_user_id
		from cp_video_session_remote_connected
		inner join episodes_subject
			using (episode_id)
		where cp_video_session_remote_connected.timestamp <= '2018-04-10'
			and cp_video_session_remote_connected.timestamp > '2017-11-01'

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
	, coalesce(practitioners.user_name, 'N/A') as user_name
	, episodes_issue_types.issue_type
from videos
left join practitioners
	on videos.careplatform_user_id = practitioners.user_id
left join test_users
	on videos.patient_id = test_users.user_id
left join episodes_issue_types
	using (episode_id)
where test_users.user_id is null
