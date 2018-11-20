with
	shifts as (
		select * from {{ ref('wiw_shifts') }}
	)

	, users_snapshot_2018_09 as (
		select * from {{ ref('wiw_users_snapshot_2018_09') }}
	)

select shifts.shift_id
	, shifts.start_time
	, shifts.end_time
	, shifts.start_time_est
	, shifts.end_time_est
	, shifts.start_date_est
	, shifts.end_date_est
	, shifts.shift_schedule
	, shifts.hours
	, shifts.break_time
	, shifts.user_id
	, shifts.wiw_user_id
	, shifts.email
	, shifts.first_name
	, shifts.last_name
	, shifts.full_name
	, shifts.position_id
	, shifts.position_name
	, shifts.location_id
	, shifts.location_name
	, coalesce(users.hourly_rate * (shifts.hours - shifts.break_time)
		, cost
	) as cost
	, coalesce(users.hourly_rate, shifts.hourly_rate) as hourly_rate
from shifts
left join users_snapshot_2018_09 as users
	on shifts.wiw_user_id = users.wiw_user_id
		and shifts.start_time_est < '2018-09-10'
