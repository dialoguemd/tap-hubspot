with
	tmp as (
		select generate_series(
			'2016-01-01',
			date_trunc('month', current_date), interval '1 month'
		) as date_month
	)

	, months as (
		select date_month
			, date_month + interval '1 month' as month_end
			, tsrange(date_month::timestamp,
		                date_month::timestamp + interval '1 month')
		                as month_range
		from tmp
	)

select *
	, {{ days_in_range("month_range") }} as days_in_month
from months
