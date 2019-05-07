with
	chats as (
		select * from {{ ref('chats') }}
	)

select date_trunc('month', date_day_est) as date_month
	, count(distinct episode_id)
		filter(where
			first_set_active is not null
			and chat_type = 'New Episode'
		)
	as episode_count
	, percentile_disc(0.9) within group (order by wait_time_first_care_team)
		filter(where is_first_message_in_opening_hours)
	as first_response_time_90p
	, median(time_to_resolved_pending)
		filter(where is_first_message_in_opening_hours)
	as time_to_resolve_median
from chats
where date_day_est < date_trunc('month', current_date)
	-- date when the resolve feature was implemented
	and date_day_est >= '2017-11-01'
group by 1
