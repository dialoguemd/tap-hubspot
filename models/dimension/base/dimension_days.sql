with
	dimension_months as (
		select * from {{ ref('dimension_months') }}
	)

	, dates as (
		select generate_series(
			'2016-01-01',
			current_date,
			interval '1 day'
		) as date_day
	)

select date_trunc('month', dates.date_day) as date_month
	, date_trunc('week', dates.date_day) as date_week
	, dates.date_day
	, dimension_months.days_in_month
from dates
inner join dimension_months
	on date_trunc('month', dates.date_day) = dimension_months.date_month
