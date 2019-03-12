with months as (
	select generate_series(
		'2016-01-01',
		date_trunc('month', current_date), interval '1 month')
		as date_month
	)

select date_month
	, tsrange(date_month::timestamp,
                date_month::timestamp + interval '1 month')
                as month_range_est
from months
