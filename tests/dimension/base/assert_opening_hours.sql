with
	wiw_opening_hours as (
		select * from {{ ref('wiw_opening_hours') }}
	)

select *
from wiw_opening_hours
where -- limited hours on a Tuesday
	(
		date_day = '2019-01-01'
		and (
			opening_hour_est <> '2019-01-01 10:00:00'
			or closing_hour_est <> '2019-01-01 16:00:00'
		)
	)
	-- regular hours
	{% for i in [2, 3, 4] -%}
	or (
		date_day = '2019-01-0{{i}}'
		and (
			opening_hour_est <> '2019-01-0{{i}} 08:00:00'
			or closing_hour_est <> '2019-01-0{{i}} 20:00:00'
		)
	)
	{% endfor -%}
	-- weekend hours
	or (
		date_day = '2019-01-05'
		and (
			opening_hour_est <> '2019-01-05 10:00:00'
			or closing_hour_est <> '2019-01-05 16:00:00'
		)
	)
	-- weekend hours
	or (
		date_day = '2019-01-06'
		and (
			opening_hour_est <> '2019-01-06 10:00:00'
			or closing_hour_est <> '2019-01-06 16:00:00'
		)
	)
	-- closed Sunday
	or date_day = '2017-09-03'
	-- reduced hours Friday
	or (
		date_day = '2017-06-09'
		and (
			opening_hour_est <> '2017-06-09 08:00:00'
			or closing_hour_est <> '2017-06-09 17:00:00'
		)
	)
