with
	shifts as (
		select * from {{ ref('wiw_shifts') }}
	)

	, users_snapshot_2018_09 as (
		select * from {{ ref('wiw_users_snapshot_2018_09') }}
	)

	, position_groups as (
		select * from {{ ref('wiw_position_groups') }}
	)

select {{
	dbt_utils.star(
		from=ref('wiw_shifts'),
		except=['hourly_rate', 'cost'],
		relation_alias='shifts',
	) }}
	, coalesce(users.hourly_rate * (shifts.hours - shifts.break_time)
		, shifts.cost
	) as cost
	, coalesce(users.hourly_rate, shifts.hourly_rate) as hourly_rate
	, position_groups.position_group
from shifts
left join position_groups
	using (position_name)
left join users_snapshot_2018_09 as users
	on shifts.wiw_user_id = users.wiw_user_id
		and shifts.start_time_est < '2018-09-10'
