select shift_id
	, user_id as wiw_user_id
	, employee_code as user_id
	, start_time
	, end_time
	, tstzrange(start_time_est, end_time_est, '[]') as shift_schedule
	, start_time_est
	, end_time_est
    , start_date_est
    , end_date_est
	, hours
	, break_time
	, email
	, first_name
	, last_name
	, first_name || ' ' || last_name as full_name
	, position_id
	, position_name
	, location_id
	, location_name
	, cost
	, hourly_rate
from wiw.shifts
