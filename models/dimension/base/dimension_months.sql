select generate_series(
	'2010-01-01',
	date_trunc('month', current_date), interval '1 month')
	as date_month
