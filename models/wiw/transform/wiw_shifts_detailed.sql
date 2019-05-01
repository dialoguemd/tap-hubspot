with
	shifts_tmp as (
		select * from {{ ref('wiw_shifts') }}
	)

	, locations as (
		select * from {{ ref('wiw_locations') }}
	)

	, positions as (
		select * from {{ ref('wiw_positions') }}
	)

	, users as (
		select * from {{ ref('wiw_users') }}
	)

	, users_snapshot_2018_09 as (
		select * from {{ ref('wiw_users_snapshot_2018_09') }}
	)

	, position_groups as (
		select * from {{ ref('wiw_position_groups') }}
	)

	, shifts as (
		select shifts_tmp.shift_id
			, shifts_tmp.wiw_user_id
			, users.user_id
			, shifts_tmp.start_day_est
			, shifts_tmp.start_week_est
			, shifts_tmp.start_month_est
			, shifts_tmp.end_day_est
			, shifts_tmp.end_week_est
			, shifts_tmp.end_month_est
			, shifts_tmp.start_time
			, shifts_tmp.end_time
			, shifts_tmp.start_time_est
			, shifts_tmp.end_time_est
			, shifts_tmp.shift_schedule
			, shifts_tmp.shift_schedule_est
			, shifts_tmp.hours
			, shifts_tmp.break_time
			, users.email
			, users.first_name
			, users.last_name
			, users.full_name
			, shifts_tmp.position_id
			, positions.position_name
			, shifts_tmp.location_id
			, locations.location_name
			, users.hourly_rate
		from shifts_tmp
		left join locations
			using (location_id)
		left join positions
			using (position_id)
		left join users
			using (wiw_user_id)
		where shifts_tmp.is_published
	)

select shifts.shift_id
	, shifts.wiw_user_id
	, shifts.user_id
	, shifts.start_day_est
	, shifts.start_week_est
	, shifts.start_month_est
	, shifts.end_day_est
	, shifts.end_week_est
	, shifts.end_month_est
	, shifts.start_time
	, shifts.end_time
	, shifts.start_time_est
	, shifts.end_time_est
	, shifts.shift_schedule
	, shifts.shift_schedule_est
	, shifts.hours
	, shifts.break_time
	, shifts.email
	, shifts.first_name
	, shifts.last_name
	, shifts.full_name
	, shifts.position_id
	, shifts.position_name
	, shifts.location_id
	, shifts.location_name
	, coalesce(users.hourly_rate * (shifts.hours - shifts.break_time)
		, (shifts.hours - shifts.break_time) * shifts.hourly_rate
	) as cost
	, coalesce(users.hourly_rate, shifts.hourly_rate) as hourly_rate
	, position_groups.position_group

from shifts
left join position_groups
	using (position_name)
left join users_snapshot_2018_09 as users
	on shifts.wiw_user_id = users.wiw_user_id
		and shifts.start_time_est < '2018-09-10'
