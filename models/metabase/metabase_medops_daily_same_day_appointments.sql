with
	medops_time_to_next_apt_or_not as (
		select * from {{ ref('medops_time_to_next_apt_or_not') }}
	)

select 1.0
	* count(*) filter(where
		triggered_at_day = video_started_day
	) / count(*) as same_day_percent
from medops_time_to_next_apt_or_not
where triggered_at_day = current_date - interval '1 day'
