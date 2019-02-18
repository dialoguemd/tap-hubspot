with user_contract as (	
        select * from {{ ref('user_contract') }}
    )
    
    , days as (
    	select * from {{ ref('dimension_days') }}
	)

	, user_contract_day as (
		select days.date_day
			, user_contract.organization_name
			, user_contract.organization_id
			, user_contract.charge_price
			, count(distinct user_id)
				filter (where user_contract.family_member_type = 'Employee')
				as count_users
		from days
		inner join user_contract
			on days.date_day <@ user_contract.during
		{{ dbt_utils.group_by(4) }}
	)

select date_trunc('month', date_day) as date_month
    , organization_name
    , organization_id
    , charge_price
    , avg(count_users) as count_users
    , avg(count_users) * charge_price as revenue
from user_contract_day
{{ dbt_utils.group_by(4) }}
