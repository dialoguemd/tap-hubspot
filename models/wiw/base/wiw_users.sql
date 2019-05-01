select id::text as wiw_user_id
	, first_name || ' ' || last_name as full_name
	, case when employee_code = ''
		then null
		else employee_code
		end as user_id
	, *
from tap_wiw.users
