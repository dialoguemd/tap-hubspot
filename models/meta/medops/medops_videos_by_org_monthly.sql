with video_stream_created as (
		select * from {{ ref( 'careplatform_video_stream_created' ) }}
    )

	, episodes as (
		select * from {{ ref( 'episodes' ) }}
    )

	select patients.organization_name
	    , date_trunc('month', video_stream_created.created_at) as month
	    , count(distinct concat(patients.user_id, date_trunc('day', video_stream_created.created_at))) as count_videos
	from video_stream_created
	left join pdt.users as practitioners
	    on video_stream_created.practitioner_id = practitioners.user_id
	left join episodes using (episode_id)
	left join pdt.users as patients
	    on episodes.user_id = patients.user_id
	where practitioners.main_specialization = 'Family Physician'
	group by 1,2
