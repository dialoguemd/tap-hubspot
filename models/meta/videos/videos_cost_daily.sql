with
	wiw_shifts as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

	, video_started as (
		select * from {{ ref('videos_started') }}
	)

	, daily_costs as (
		select start_day_est as date
			, sum(cost) as cost
		from wiw_shifts
		where position_name in ('GP', 'Nurse Practitioner')
			and location_name = 'Virtual Care Platform'
			and start_date_est
				< date_trunc('week', current_date)
		group by 1
	)

select daily_costs.date
	, daily_costs.cost
	, count(video_started.episode_id) as count_videos
	, daily_costs.cost
		/ count(video_started.episode_id)::float as cost_per_video
from daily_costs
inner join video_started
	on daily_costs.date = date_trunc('day', video_started.timestamp)
	and main_specialization in ('Family Physician', 'Nurse Practitioner')
group by 1,2
