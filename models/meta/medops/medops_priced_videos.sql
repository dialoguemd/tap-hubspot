with videos as (
        select * from {{ ref( 'videos_by_ep_daily' ) }}
    )

    , monthly_video_cost as (
        select * from {{ ref( 'medops_video_cost_monthly' ) }}
    )

    , episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

select videos.episode_id
    , videos.date_day as date
    , count(*) filter(where episodes.issue_type = 'psy')
		* monthly_video_cost.per_video_cost * 2 as gp_psy_cost
    , count(*) filter(where episodes.issue_type <> 'psy'
			or episodes.issue_type is null)
		* monthly_video_cost.per_video_cost as gp_other_cost
from videos
left join episodes using (episode_id)
left join monthly_video_cost
	on date_trunc('month', videos.date_day) = monthly_video_cost.month
where videos.includes_video_gp
group by 1,2,per_video_cost
