with
	costs_hourly_by_spec_monthly as (
		select * from {{ ref('costs_hourly_by_spec_monthly') }}
	)

select *
from costs_hourly_by_spec_monthly
where date_month > '2018-06-01'
	and (
		cc_hourly not between 35 and 60
		or cc_hourly_ops not between 50 and 75
		or nc_hourly not between 60 and 105
		or nc_hourly_ops not between 80 and 135
	)
