with
	wiw_shifts as (
		select * from {{ ref('wiw_shifts_detailed') }}
	)

	, practitioners as (
		select * from {{ ref('practitioners') }}
	)

select {{
	dbt_utils.star(
		from=ref('wiw_shifts_detailed'),
		relation_alias='wiw_shifts',
	) }}

	, {{
		dbt_utils.star(
			from=ref('practitioners'),
			except=['email', 'user_id', 'first_name', 'last_name'],
			relation_alias='practitioners'
		)
	}}

from wiw_shifts
left join practitioners
	using (user_id)
