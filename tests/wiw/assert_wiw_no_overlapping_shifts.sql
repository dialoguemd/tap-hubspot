with
	wiw_shifts as (
        select * from {{ ref('wiw_shifts') }}
        # Don't test future shifts because they're more likely to be
        # overlapping or for some other reason not finalized
        where start_time < current_date
    )

select wiw_shifts.wiw_user_id
    , wiw_shifts.shift_schedule
    , wiw_shifts.shift_id
    , count(wiw_shifts.*)
    , count(other_shifts.*)
from wiw_shifts
inner join wiw_shifts as other_shifts
	-- Remove one second so that back-to-back shifts aren't counted
    on tstzrange(wiw_shifts.start_time, wiw_shifts.end_time - interval '1 second')
        && tstzrange(other_shifts.start_time, other_shifts.end_time - interval '1 second')
    and wiw_shifts.wiw_user_id = other_shifts.wiw_user_id
-- Don't return shifts that are joined to themselves
where wiw_shifts.shift_id <> other_shifts.shift_id
group by 1,2,3
