with
	chats_all_time as (
		select * from {{ ref('chats_all_time') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

select user_contract.organization_id
	, user_contract.organization_name
	, date_trunc('month', chats_all_time.created_at_day) as date_month
	, count(chats_all_time.user_id) as count_dau
from chats_all_time
inner join user_contract
	on chats_all_time.user_id = user_contract.user_id
	and chats_all_time.created_at_day <@ user_contract.during_est
group by 1,2,3
