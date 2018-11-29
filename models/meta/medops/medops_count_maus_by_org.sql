with
	chats_all_time as (
		select * from {{ ref('chats_all_time') }}
	)

	, user_contract as (
		select * from {{ ref('scribe_user_contract_detailed') }}
	)

select user_contract.organization_id
	, user_contract.organization_name
	, date_trunc('month', chats_all_time.created_at_day) as date_month
	, count(distinct user_contract.user_id) as count_mau
from chats_all_time
inner join user_contract
	on chats_all_time.user_id = user_contract.user_id
	and chats_all_time.created_at_day <@ user_contract.during_est
-- Before this date not all users had organizations
where chats_all_time.created_at_day > '2017-10-01'
group by 1,2,3
