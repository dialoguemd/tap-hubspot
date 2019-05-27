with
	ranked as (
		select previous_id as anonymous_id
			, user_id
			, row_number()
				over (partition by previous_id order by timestamp)
				as rank
		from patientapp.aliases
		where user_id is not null
	)

select *
from ranked
where rank = 1
