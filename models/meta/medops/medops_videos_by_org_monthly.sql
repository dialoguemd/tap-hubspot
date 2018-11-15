with videos as (
		select * from {{ ref( 'videos_by_ep_daily' ) }}
    )

    , users as (
		select * from {{ ref( 'user_contract' ) }}
    )

select patients.organization_name
    , date_trunc('month', videos.date_day_est) as date_month
    , count(distinct
		concat(patients.user_id,
				date_trunc('day',
				videos.date_day_est)
			)
		) as count_videos
from videos
inner join users as patients
	on videos.patient_id = patients.user_id
	and tsrange(timezone('America/Montreal', lower(patients.during)),
		timezone('America/Montreal', upper(patients.during)))
		@> videos.date_day_est
where videos.includes_video_gp
group by 1,2
