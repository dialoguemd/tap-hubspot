with
	shifts as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

	, aggregated as (
		select start_week_est
			, count(*) filter (where user_id is null) * 1.0
				/ count(*) as fraction_without_employee_code
		from shifts
		where start_time > '2018-10-01'
			and position_group in ('Care Coordinator', 'Nurse Clinician')
		group by 1
	)

select *
from aggregated
where fraction_without_employee_code > 0.01
