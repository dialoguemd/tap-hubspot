select generate_series(
	'2016-01-01',
	current_date,
	interval '1 day')
	as date_day
