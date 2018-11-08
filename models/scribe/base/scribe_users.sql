select id as user_id
	, address_country
	, address_state
	, age
	, case when birthday is null
		then created_at - interval '1 year' * age
		else birthday
	end as birthday
	, country
	, created_at
	, email
	, first_name
	, last_name
	, first_name || ' ' || last_name as user_name
	, gender
	, language
	, residence_province
	, auth_id
	, coalesce(status, 'invited') as status
from scribe.users
