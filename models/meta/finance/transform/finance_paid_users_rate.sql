with
	active_users as (
		select * from {{ ref('active_users_monthly')}}
	)

select date_month
	-- Use the paid users rate to determine rebate
	, active_users.daus_paid::float / active_users.daus as paid_users_rate
	, 1 - (active_users.daus_paid::float / active_users.daus) as rebate
from active_users
