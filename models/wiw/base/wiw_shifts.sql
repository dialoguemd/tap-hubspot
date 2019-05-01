with
	shifts as (
		select id as shift_id
			, tstzrange(start_time, end_time,'[]') as shift_schedule
			, tsrange(timezone('America/Montreal', start_time),
						timezone('America/Montreal', end_time),
						'[]') as shift_schedule_est
			, start_time
			, end_time
			, timezone('America/Montreal', start_time) as start_time_est
			, timezone('America/Montreal', end_time) as end_time_est
			, extract(epoch from end_time - start_time) / 3600 as hours
			, break_time::float
			, location_id
			, position_id
			, published as is_published
			, user_id::text as wiw_user_id
		from tap_wiw.shifts
		-- exclude duplicate shifts
		where id not in
			('1216735738',
			'1842146641',
			'1842148236',
			'1842149906',
			'2009841786',
			'2073341495',
			'2091530628',
			'1963137827')
	)

select *
	{% for timeframe in ['day', 'week', 'month'] %}

	, date_trunc('{{timeframe}}', start_time_est)
		as start_{{timeframe}}_est
	, date_trunc('{{timeframe}}', end_time_est)
		as end_{{timeframe}}_est

	{% endfor %}
from shifts
