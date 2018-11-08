with chats_all_time as (
    	select * from {{ ref( 'chats_all_time' ) }}
    )

	, users as (
		select * from pdt.users
	)

	select organization_name
	    , date_trunc('month', created_at_day) as month
	    , count(*) as count_dau
	from chats_all_time
	left join users using (user_id)
	where chats_all_time.user_id is not null
	group by 1,2
