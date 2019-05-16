select episode_id
	, command_name
	, user_id
	, case
		when command_id = 'Outcome Counselling'
		then 'Outcome Nurse Counselling'
		else command_id
	end as command_id
	, timestamp as triggered_at
	, timestamp
	, {{ to_est() }}
from careplatform.executed_command
