with
	videos as (
		select * from {{ ref('videos_by_episode_daily') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

select user_contract.organization_id
	, user_contract.organization_name
	, date_trunc('month', videos.date_day_est) as date_month
	, count(distinct
		concat(user_contract.user_id,
				date_trunc('day',
				videos.date_day_est)
			)
		) as count_videos
from videos
inner join user_contract
	on videos.patient_id = user_contract.user_id
	and videos.date_day_est <@ user_contract.during_est
where videos.includes_video_gp
group by 1,2,3
