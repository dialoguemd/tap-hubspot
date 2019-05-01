-- not a CSV to keep salary data outside of this repo
select employee_code as user_id
	, user_id::text as wiw_user_id
	, email
	, first_name
	, last_name
	, hourly_rate
from wiw.users_snapshot_2018_09
