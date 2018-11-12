with
	shifts as (
		select * from {{ ref('wiw_shifts') }}
	)

select date_trunc('month', start_date_est) as start_month
	, sum(hours) / 160.0 as ftes
from shifts
where location_name = 'Virtual Care Platform'
group by 1
