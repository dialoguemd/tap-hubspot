-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

--

with chats as (
		select * from {{ ref('chats') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

	, paid_employees_monthly as (
		select * from {{ ref('users_paid_employees_monthly') }}
	)

	, activity as (
		select user_contract.organization_id
			, user_contract.organization_name
			, date_trunc('month', chats.created_at_day) as date_month
			, count(distinct chats.user_id) as mau_count
			, count(distinct
				concat(chats.user_id, chats.episode_id)
				) as dau_count
		from chats
		inner join user_contract
			on chats.user_id = user_contract.user_id
			and chats.created_at_day <@ user_contract.during_est
		-- Before this date not all users had organizations
		where chats.created_at_day > '2017-10-01'
		group by 1,2,3
	)

select paid_employees_monthly.date_month
	, paid_employees_monthly.organization_id
	, paid_employees_monthly.organization_name
	, paid_employees_monthly.account_name
	, paid_employees_monthly.count_paid_employees
	, activity.mau_count
	, activity.dau_count
	, case
		when paid_employees_monthly.count_paid_employees <> 0
		then coalesce(activity.mau_count, 0)
			/ paid_employees_monthly.count_paid_employees::float
		else 0
	end as mau_rate
	, case
		when paid_employees_monthly.count_paid_employees <> 0
		then coalesce(activity.dau_count, 0)
			/ paid_employees_monthly.count_paid_employees::float
		else 0
	end as dau_rate
from paid_employees_monthly
left join activity
	using (date_month, organization_id)
