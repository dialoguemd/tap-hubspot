with
	users as (
		select * from {{ ref('scribe_users') }}
	)

	, user_signed_up as (
		select * from {{ ref('scribe_user_signed_up') }}
	)

	, child_added as (
		select * from {{ ref('scribe_child_added') }}
	)

	, user_first_sign_up as (
		select user_id
			, min(timestamp) as signed_up_at
		from user_signed_up
		group by 1
	)

select users.*
	, coalesce(
		user_first_sign_up.signed_up_at
		, case
		-- legacy signup system
			when users.status = 'registered' or users.auth_id is not null
			then created_at
			else null
		end
	) as signed_up_at
	, user_first_sign_up.signed_up_at is not null
		or users.status = 'registered'
		or users.auth_id is not null
	as is_signed_up
	, child_added.child_id is not null as is_child
from users
left join user_first_sign_up
	using (user_id)
left join child_added
	on users.user_id = child_added.child_id
