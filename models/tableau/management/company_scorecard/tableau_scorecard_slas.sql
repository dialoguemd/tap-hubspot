with
	dates as (
		select * from {{ ref('dimension_scorecard_weeks') }}
	)

	, chats as (
		select * from {{ ref('chats') }}
	)

select dates.date_week
	, percentile_disc(0.9) within group (order by chats.frt_care_team)
		as frt_90th_percentile
	, percentile_disc(0.9)
		within group (order by chats.frt_nurse)
		filter(where chats.chat_type = 'New Episode')
		as frt_nurse_90th_percentile
 	, percentile_disc(0.5)
 		within group (order by chats.time_to_resolved_pending)
 		as ttr_median
from dates
left join chats
	on dates.date_week = chats.date_week_est
		and chats.is_first_message_in_opening_hours
		and chats.date_week_est < date_trunc('week', current_date)
group by 1
