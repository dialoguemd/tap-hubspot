with
	user_activated as (
		select * from {{ ref('user_activated') }}
	)

	, active_users as (
		select * from {{ ref('active_users') }}
	)

	, active_month as (
		select user_id
			, organization_id
			, organization_name
			, account_id
			, account_name
			, family_member_type
			, date_month
		from active_users
		group by 1,2,3,4,5,6,7
	)

select active_month.user_id
	, active_month.organization_id
	, active_month.organization_name
	, active_month.account_id
	, active_month.account_name
	, case
		when user_activated.user_id is null
		then 'Repeat'
		else 'New'
	end || ' ' ||
		case
		when family_member_type in ('Child', 'Dependent')
		then 'Family'
		else 'Employee'
	end as activation_type
	, active_month.date_month
from active_month
left join user_activated
	on active_month.user_id = user_activated.user_id
	and active_month.date_month
		= date_trunc('month', user_activated.activated_at)
