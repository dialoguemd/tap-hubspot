with
	active_users as (
		select * from {{ ref('active_users') }}
	)

select user_id
	, min(first_activity_created_at) as activated_at
from active_users
group by 1
