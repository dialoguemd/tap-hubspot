with
	costs_hourly_by_spec_monthly as (
		select * from {{ ref('costs_hourly_by_spec_monthly') }}
	)

select *
from costs_hourly_by_spec_monthly
-- Calibrated in April 2019
where date_month >= '2019-02-01'
	and (
		cc_hourly not between 35 and 45
		or cc_hourly_ops not between 50 and 60
		or nc_hourly not between 65 and 80
		or nc_hourly_ops not between 85 and 105
	)
