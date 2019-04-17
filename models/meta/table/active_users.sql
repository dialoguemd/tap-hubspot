with
	active_users as (
		select * from {{ ref('active_users_unfiltered')}}
	)

select *
from active_users
where set_active
