with
	videos as (
		select * from {{ ref('videos_detailed') }}
	)

select date_trunc('month', date_day_est) as date_month
	, count(*)
		filter(where issue_type = 'psy') as psy_video_count
	, count(*)
		filter(where issue_type <> 'psy'
			or issue_type is null) as other_video_count
from videos
where videos.main_specialization in ('Family Physician', 'Psychologist')
    -- Only count videos that are greater than 2 minutes
    and videos.video_length > 2
group by 1
