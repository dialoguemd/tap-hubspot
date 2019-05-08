select coalesce(user_id, anonymous_id) as user_id
	, timestamp
from patientapp.login_fetch_complete
