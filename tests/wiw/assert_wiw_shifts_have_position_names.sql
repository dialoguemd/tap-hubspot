with
	shifts as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

	, aggregated as (
		select start_week_est
			, count(*) filter (where position_name is null) * 1.0
				/ count(*) as fraction_without_position_name
		from shifts
		where start_time > '2019-01-01'
		group by 1
	)

select *
from aggregated
where fraction_without_position_name > 0.01
