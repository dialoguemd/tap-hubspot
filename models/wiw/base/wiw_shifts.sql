with
	shifts as (
		select shift_id
			, user_id as wiw_user_id
			, employee_code as user_id
			, start_time
			, end_time
			, tstzrange(start_time, end_time,'[]') as shift_schedule
			, tsrange(timezone('America/Montreal', start_time),
						timezone('America/Montreal', end_time),
						'[]') as shift_schedule_est
			, timezone('America/Montreal', start_time) as start_time_est
			, timezone('America/Montreal', end_time) as end_time_est
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
	)

select *
	, date_trunc('day', start_time_est)
		as start_date_est
	, date_trunc('day', end_time_est)
		as end_date_est

	{% for timeframe in ['day', 'week', 'month'] %}

	, date_trunc('{{timeframe}}', start_time_est)
		as start_{{timeframe}}_est
	, date_trunc('{{timeframe}}', end_time_est)
		as end_{{timeframe}}_est

	{% endfor %}

from shifts
