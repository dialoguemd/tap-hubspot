with videos as (
		select * from {{ ref( 'videos_by_ep_daily' ) }}
	)

	, episodes as (
		select * from {{ ref( 'episodes' ) }}
	)

	, organizations as (
		select * from {{ ref( 'organizations' ) }}
	)

	select date_trunc('month', date_day) as month
		, count(*)
			filter(where issue_type = 'psy') as psy_video_count
		, count(*)
			filter(where issue_type <> 'psy'
				or issue_type is null) as other_video_count
	from videos
	left join episodes using (episode_id)
	left join organizations using (organization_id)
	where videos.includes_video_gp
	group by 1
