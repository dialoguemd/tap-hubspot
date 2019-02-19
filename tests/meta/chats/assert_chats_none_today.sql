with
	chats as (
		select * from {{ ref('chats') }}
	)

select * from chats
where date_day_est
	= date_trunc('day', current_timestamp)