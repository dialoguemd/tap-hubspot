with chats_all_time as (
    	select * from {{ ref( 'chats_all_time' ) }}
    )

	, users as (
		select * from {{ ref( 'user_contract' ) }}
	)

select users.organization_name
    , date_trunc('month', chats_all_time.created_at_day) as date_month
    , count(chats_all_time.user_id) as count_dau
from chats_all_time
inner join users
		on chats_all_time.user_id = users.user_id
		and chats_all_time.created_at_day <@
			tsrange(timezone('America/Montreal', lower(users.during)),
			timezone('America/Montreal', upper(users.during)))
group by 1,2
