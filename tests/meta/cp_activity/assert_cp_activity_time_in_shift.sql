with
	cp_activity as (
		select * from {{ ref('cp_activity') }}
	)

	, wiw_shifts_raw as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

	, wiw_shifts as (
		select *
		from wiw_shifts_raw
		where start_day_est < current_date
			and location_name = 'Virtual Care Platform'
			and position_name not in (
				'Evening shift nurse', 'Medical Archivist', 'Psychologist',
				'Night Shift Nurse', 'Nutritionist', 'Psychotherapist'
			)
	)

	, shift_activity as (
		select shift_id
			, sum(time_spent) filter(where is_active) as time_spent_active
			, sum(time_spent) as time_spent
		from cp_activity
		group by 1
	)

	, shift_summary as (
		select wiw_shifts.shift_id
			, wiw_shifts.start_week_est
			, hours * 3600 as shift_seconds
			, sum(time_spent_active) as time_spent_active
			, sum(time_spent) as time_spent
		from wiw_shifts
		left join shift_activity
			using (shift_id)
		group by 1,2,3
	)

select start_week_est
	, sum(time_spent_active) / sum(shift_seconds) as active_ratio
	, sum(time_spent) / sum(shift_seconds) as tracked_ratio
from shift_summary
where start_week_est >= '2018-11-05'
group by 1
-- Calibrated on 2019-05-02
having sum(time_spent_active) / sum(shift_seconds) < .65
	or sum(time_spent) / sum(shift_seconds) < .90
