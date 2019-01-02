with videos as (
        select * from {{ ref( 'videos' ) }}
    )

    , monthly_video_cost as (
        select * from {{ ref( 'videos_cost_monthly' ) }}
    )

select videos.episode_id
    , videos.date_day_est as date
    , videos.patient_id
    , count(*) filter(where videos.issue_type = 'psy')
		* monthly_video_cost.per_video_cost * 2 as gp_psy_cost
    , count(*) filter(where videos.issue_type <> 'psy'
			or videos.issue_type is null)
		* monthly_video_cost.per_video_cost as gp_other_cost
from videos
left join monthly_video_cost
	on date_trunc('month', videos.date_day_est) = monthly_video_cost.date_month
where videos.main_specialization in ('Family Physician', 'Psychologist')
    -- Only count videos that are greater than 2 minutes
    and videos.video_length > 2
group by 1,2,3,per_video_cost
