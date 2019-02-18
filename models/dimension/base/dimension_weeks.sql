with dates as(
	select generate_series(
		'2016-01-04', -- the first monday of 2016
		current_date,
		interval '1 week')
		as date_week
	)

select date_week
	, tstzrange(date_week, date_week + interval '1 week') as week_range
from dates
