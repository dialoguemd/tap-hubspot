with chats_all_time as (
    	select * from {{ ref( 'chats_all_time' ) }}
    )

	, users as (
		select * from {{ ref( 'user_contract' ) }}
	)

select users.organization_name
    , date_trunc('month', chats_all_time.created_at_day) as date_month
    , count(distinct users.user_id) as count_mau
from chats_all_time
inner join users using (user_id)
-- Before this date not all users had organizations
where created_at_day > '2017-10-01'
group by 1,2
