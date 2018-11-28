with
	practitioners as (
		select * from {{ ref('coredata_practitioners') }}
	)

	, users as (
		select * from {{ ref('scribe_users') }}
	)

select *
from practitioners
inner join users
	using (user_id)
