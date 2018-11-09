with videos as (
		select * from {{ ref( 'videos_daily' ) }}
    )

	, episodes as (
		select * from {{ ref( 'episodes' ) }}
    )

    , users as (
		select * from {{ ref( 'user_contract' ) }}
    )

select patients.organization_name
    , date_trunc('month', videos.date_day) as month
    , count(distinct
		concat(patients.user_id,
				date_trunc('day',
				videos.date_day)
			)
		) as count_videos
from videos
left join episodes using (episode_id)
left join users as patients
    on episodes.user_id = patients.user_id
    and users.during @> videos.date_day
where videos.includes_video_gp
group by 1,2
