with chats_all_time as (
    	select * from {{ ref( 'chats_all_time' ) }}
    )

	, users as (
		select * from pdt.users
	)

	select organization_name
	    , date_trunc('month', created_at_day) as month
	    , count(distinct user_id) as count_mau
	from chats_all_time
	inner join users using (user_id)
	group by 1,2
