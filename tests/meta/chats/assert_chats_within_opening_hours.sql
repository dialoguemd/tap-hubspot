with
	chats as (
		select * from {{ ref('chats') }}
	)

	, aggregate as (
		select date_week_est
		    , 1.0 * count(*) filter(where is_first_message_in_opening_hours)
		        / count(*) as percentage_within_opening_hours
		    , count(*) as chat_count
		from chats
		group by 1
	)

select *
from aggregate
where percentage_within_opening_hours < .8
	and date_week_est > '2018-04-02'
