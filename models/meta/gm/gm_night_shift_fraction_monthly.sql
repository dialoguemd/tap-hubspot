with
	wiw_shifts as (
		select * from {{ ref('wiw_shifts') }}
	)

	, practitioners as (
		select * from {{ ref('practitioners') }}
	)

select date_trunc('month', start_date_est) as month
	, sum(cost) filter (where position_name = 'Night Shift Nurse')
		/ sum(cost) :: float as night_shift_fraction
from wiw_shifts
left join practitioners using (user_id)
where main_specialization = 'Nurse Clinician'
    and location_name = 'Virtual Care Platform'
group by 1
