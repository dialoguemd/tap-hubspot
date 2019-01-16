with
	video_start as (
		select * from {{ ref('videos_started') }}
	)

	, video_stream_ended as (
		select * from {{ ref('careplatform_video_stream_ended') }}
	)

	, video_end as (
		select episode_id
			, practitioner_id as careplatform_user_id
			, timestamp_est
		from video_stream_ended
		-- Only include videos since the April 2018 refactor
		where timestamp_est > '2018-04-10'
	)

select md5(
		concat(video_start.patient_id, video_start.timestamp)
		) as video_id
	, video_start.episode_id
	, video_start.patient_id
    , video_start.careplatform_user_id
    , video_start.main_specialization
    , video_start.user_name
    , video_start.issue_type
    , video_start.date_day_est
    , video_start.timestamp as started_at
    , video_start.timestamp_est as started_at_est
    , min(video_end.timestamp_est) as ended_at_est
    , extract(epoch from min(video_end.timestamp_est)
		- video_start.timestamp_est)/60 as video_length
from video_start
-- Only include records for videos with both start and end timestamps
inner join video_end
	on video_start.episode_id = video_end.episode_id
	and video_start.careplatform_user_id
		= video_end.careplatform_user_id
	and tsrange(video_start.timestamp_est,
			video_start.timestamp_est + interval '90 minutes')
		@> video_end.timestamp_est
-- Only include videos since the April 2018 refactor
where video_start.timestamp_est > '2018-04-10'
group by 1,2,3,4,5,6,7,8,9,10
