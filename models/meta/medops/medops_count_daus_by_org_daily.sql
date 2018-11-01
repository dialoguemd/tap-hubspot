with chats_all_time as (
    select * from {{ ref( 'pdt_chats_all_time' ) }}
    )

	select organization_name
	    , date_trunc('day', created_at_day) as day
	    , count(distinct user_id) as count_dau
	from chats_all_time
	inner join pdt.users using (user_id)
	group by 1,2
