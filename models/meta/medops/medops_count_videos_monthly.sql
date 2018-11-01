with careplatform_video_activity as (
    	select * from {{ ref( 'careplatform_video_activity' ) }}
    )

	select date_trunc('month', date) as month
	    , count(*) filter(where issue_type = 'psy') as psy_video_count
	    , count(*) filter(where issue_type <> 'psy') as other_video_count
	from careplatform_video_activity
	left join pdt.episodes using (episode_id)
	group by 1
