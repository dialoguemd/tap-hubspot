with
	shifts as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

select start_day_est as start_date
	, position_name
	, sum(cost) as cost
	, sum(hours) as hours_worked

	, sum(cost)
		filter(where location_name = 'Virtual Care Platform')
			as virtual_costs
	, sum(hours)
		filter(where location_name = 'Virtual Care Platform')
			as virtual_hours

	, sum(cost)
		filter(where location_name = 'Admin')
			as admin_costs
	, sum(hours)
		filter(where location_name = 'Admin')
			as admin_hours

	, sum(cost)
		filter(where location_name = 'Ubisoft')
			as ubisoft_costs
	, sum(hours)
		filter(where location_name = 'Ubisoft')
			as ubisoft_hours
from shifts
where position_name is not null
group by 1,2
