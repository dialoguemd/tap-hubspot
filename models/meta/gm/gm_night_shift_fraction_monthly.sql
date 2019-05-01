with
	wiw_shifts as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

	, practitioners as (
		select * from {{ ref('practitioners') }}
	)

select date_trunc('month', wiw_shifts.start_date_est) as month
	, sum(wiw_shifts.cost) filter (where wiw_shifts.position_name = 'Night Shift Nurse')
		/ sum(wiw_shifts.cost) :: float as night_shift_fraction
from wiw_shifts
left join practitioners using (user_id)
where practitioners.main_specialization = 'Nurse Clinician'
    and wiw_shifts.location_name = 'Virtual Care Platform'
group by 1
