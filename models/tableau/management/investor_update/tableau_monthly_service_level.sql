with
	chats as (
		select * from {{ ref('chats') }}
	)

select date_trunc('month', created_at_day) as date_month
	, count(distinct episode_id)
		filter(where chat_type = 'New Episode')
	as episode_count
	, percentile_disc(0.9) within group (order by time_since_last_message)
		filter(where is_first_message_in_opening_hours)
	as first_response_time_90p
	, median(time_to_resolved_pending)
		filter(where is_first_message_in_opening_hours)
	as time_to_resolve_median
from chats
where created_at_day < date_trunc('month', current_date)
	-- date when the resolve feature was implemented
	and created_at_day >= '2017-11-01'
group by 1
