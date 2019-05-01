with
	shifts as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

select start_month_est as start_month
	, sum(hours) / 160.0 as ftes
from shifts
where location_name = 'Virtual Care Platform'
group by 1
