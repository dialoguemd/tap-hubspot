with
	chats as (
		select * from {{ ref('chats') }}
	)

select percentile_disc(.9) within group (order by frt_care_team)
		as frt_care_team_90p
	, percentile_disc(.5) within group (order by time_to_resolved_pending)
		as time_to_resolved_pending_median
from chats
where date_day_est = current_date - interval '1 day'
