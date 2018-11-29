with
	video_start as (
		select * from {{ ref('video_started') }}
	)

	, video_stream_ended as (
		select * from {{ ref('careplatform_video_stream_ended') }}
	)

	, practitioners as (
		select * from {{ ref('practitioners') }}
	)

	, episodes_subject as (
		select * from {{ ref('episodes_subject') }}
	)

	, episodes_issue_types as (
		select * from {{ ref('episodes_issue_types') }}
	)

	, video_end as (
		select episode_id
			, practitioner_id as careplatform_user_id
			, timestamp_est
		from video_stream_ended
		-- Only include videos since the April 2018 refactor
		where timestamp_est > '2018-04-10'
	)

	, videos as (
		select video_start.episode_id
			, video_start.patient_id
		    , video_start.careplatform_user_id
		    , video_start.date_day_est
		    , video_start.timestamp_est
		    , video_start.main_specialization
		    , extract(epoch from min(video_end.timestamp_est)
		    	- video_start.timestamp_est)/60 as video_length
		from video_start
		left join video_end
	    	on video_start.episode_id = video_end.episode_id
	    	and video_start.careplatform_user_id
	    		= video_end.careplatform_user_id
	    	and tsrange(video_start.timestamp_est,
	    			video_start.timestamp_est + interval '90 minutes')
	    		@> video_end.timestamp_est
	    -- Only include videos since the April 2018 refactor
	    where video_start.timestamp_est > '2018-04-10'
	    group by 1,2,3,4,5,6
	)

select videos.date_day_est
	, videos.careplatform_user_id
	, practitioners.user_name
	, videos.episode_id
	, videos.main_specialization
	, coalesce(episodes_subject.episode_subject, videos.patient_id)
		as patient_id
	, videos.timestamp_est
	, videos.video_length
	, episodes_issue_types.issue_type
from videos
left join episodes_subject
	using (episode_id)
left join episodes_issue_types
	using (episode_id)
left join practitioners
	on videos.careplatform_user_id = practitioners.user_id
