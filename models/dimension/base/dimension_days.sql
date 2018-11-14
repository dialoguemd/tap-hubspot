with dates as (
	select generate_series(
	'2016-01-01',
	current_date,
	interval '1 day')
	as date_day
)

select date_day
	, extract('day' from
        date_trunc('month', date_day+interval '1 month')
        - date_trunc('month', date_day)) as days_in_month
from dates
