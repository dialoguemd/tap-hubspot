select previous_id as anonymous_id
	, user_id
from patientapp.aliases
where user_id is not null
group by 1,2
