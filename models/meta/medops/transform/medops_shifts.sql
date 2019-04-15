with
	wiw_shifts as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

	, practitioners as (
		select * from {{ ref('practitioners') }}
	)

select *
from wiw_shifts
left join practitioners
	using (user_id)
