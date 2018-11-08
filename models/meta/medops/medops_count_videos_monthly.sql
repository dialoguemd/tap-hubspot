with careplatform_video_activity as (
    	select * from {{ ref( 'careplatform_video_activity' ) }}
    )

	, episodes as (
		select * from {{ ref( 'episodes' ) }}
	)

	, organizations as (
		select * from {{ ref( 'organizations' ) }}
	)

	select date_trunc('month', date) as month
	    , count(*) filter(where issue_type = 'psy') as psy_video_count
	    , count(*) filter(where issue_type <> 'psy') as other_video_count
	from careplatform_video_activity
	left join episodes using (episode_id)
	left join organizations using (organization_id)
	group by 1
