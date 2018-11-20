select generate_series(
	date_trunc('week'
		, (date_trunc('quarter', current_date)
			- interval '3 month')::timestamptz)
	, date_trunc('week'
		, (date_trunc('quarter', current_date)
			+ interval '3 month')::timestamptz)
	, '1 week'
) as date_week
