select id as user_id
	, address_country
	, address_state
	, age
	, birthday
	, country
	, created_at
	, email
	, first_name
	, last_name
	, first_name || ' ' || last_name as user_name
	, gender
	, language
	, residence_province
from scribe.users
